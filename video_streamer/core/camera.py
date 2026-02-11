import time
import logging
import struct
import sys
import os
import io
import multiprocessing
import multiprocessing.queues
import requests
import redis
import json
import base64
from datetime import datetime
from redis.exceptions import ConnectionError as RedisConnectionError
import cv2
import numpy as np

from typing import Union, IO, Tuple
from urllib.parse import urlparse

from PIL import Image

from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from video_streamer.core.config import AuthenticationConfiguration

class Camera:
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False, redis: str = None, redis_channel: str = None):
        self._device_uri = device_uri
        self._sleep_time = sleep_time
        self._debug = debug
        self._width = -1
        self._height = -1
        self._output = None
        self._redis = redis
        self._redis_channel = redis_channel

    def _poll_once(self) -> None:
        pass

    def _write_data(self, data: bytearray):
        if isinstance(self._output, multiprocessing.queues.Queue):
            self._output.put(data)
        else:
            self._output.write(data)

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output
        if self._redis:
            host, port = self._redis.split(':')
            self._redis_client = redis.StrictRedis(host=host, port=int(port))

        while True:
            try:
                self._poll_once()
            except KeyboardInterrupt:
                sys.exit(0)
            except BrokenPipeError:
                sys.exit(0)
            except Exception:
                logging.exception("")
            finally:
                pass

    @property
    def size(self) -> Tuple[int, int]:
        return (self._width, self._height)

    def get_jpeg(self, data, size=(0, 0), v_flip=False) -> bytearray:
        jpeg_data = io.BytesIO()
        image = Image.frombytes("RGB", self.size, data, "raw")

        if size[0]:
            image = image.resize(size)

        if v_flip:
            image = image.transpose(Image.FLIP_TOP_BOTTOM)

        image.save(jpeg_data, format="JPEG")
        jpeg_data = jpeg_data.getvalue()

        return bytearray(jpeg_data)
    
    def _image_to_rgb24(self, image: bytes) -> bytearray:
        """
        Convert binary image data into raw RGB24-encoded byte array
        Supported image types include JPEG, PNG, BMP, TIFF, GIF, ...
        """
        image_array = np.frombuffer(image, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return bytearray(rgb_frame.tobytes())


class MJPEGCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, auth_config: AuthenticationConfiguration, debug: bool = False, redis: str = None, redis_channel: str = None):
        super().__init__(device_uri, sleep_time, debug, redis, redis_channel)
        self._authentication=self._createAuthenticationHeader(auth_config)
        self._set_size()

    def _set_size(self) -> None:
        buffer = bytearray()
        # To set the size, extract the first image from the MJPEG stream
        try:
            response = requests.get(self._device_uri, stream=True, verify=False, auth=self._authentication)
            if response.status_code == 200:
                boundary = self._extract_boundary(response.headers)
                if not boundary:
                    logging.error("Boundary not found in Content-Type header.")
                    return

                for chunk in response.iter_content(chunk_size=8192):
                    buffer.extend(chunk)

                    while True:
                        frame, buffer = self._extract_frame(buffer, boundary)
                        if frame is None:
                            break
                        image = Image.open(io.BytesIO(frame))
                        self._width, self._height = image.size
                        return
            else:
                logging.error(f"Received unexpected status code {response.status_code}")
                return
        except requests.RequestException as e:
            logging.exception(f"Exception occured during stream request")
            return

    def _createAuthenticationHeader(self, auth_config:AuthenticationConfiguration) -> Union[None, HTTPBasicAuth, HTTPDigestAuth]:
        type = auth_config.type
        if type == "Basic":
            return HTTPBasicAuth(username=auth_config.username, password=auth_config.password)
        elif type == "Digest":
            return HTTPDigestAuth(username=auth_config.username, password=auth_config.password)
        elif type:
            logging.warning("Unknown authentication Type {type}")
        return None

    def _extract_boundary(self, headers):
        """
        Extract the boundary marker from the Content-Type header.
        """
        content_type = headers.get("Content-Type", "")
        if "boundary=" in content_type:
            return content_type.split("boundary=")[-1]
        return None

    def _extract_frame(self, buffer: bytearray, boundary: str):
        """
        Extract a single JPEG frame from the buffer if a complete frame exists.
        Returns a tuple of (frame_data, remaining_buffer).
        """
        boundary_body_prefix = "--" if not boundary.startswith("--") else ""
        boundary_bytes = f"{boundary_body_prefix}{boundary}".encode()
        start_index = buffer.find(boundary_bytes)
        if start_index == -1:
            return None, buffer  # Boundary not found

        # Find the next boundary after the current one
        next_index = buffer.find(boundary_bytes, start_index + len(boundary_bytes))
        if next_index == -1:
            return None, buffer  # Complete frame not yet available

        # Extract the data between boundaries
        frame_section = buffer[start_index + len(boundary_bytes):next_index]

        # Separate headers and JPEG data
        header_end = frame_section.find(b"\r\n\r\n")  # End of headers
        if header_end == -1:
            return None, buffer  # Headers not fully received

        # Extract the JPEG data
        frame_data = frame_section[header_end + 4:]  # Skip past the headers
        remaining_buffer = buffer[next_index:]  # Data after the next boundary
        return frame_data.strip(), remaining_buffer  # Strip any extra whitespace

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        buffer = bytearray()
        self._output = output

        while True:
            try:
                response = requests.get(self._device_uri, stream=True, verify=False, auth=self._authentication)
                if response.status_code == 200:
                    boundary = self._extract_boundary(response.headers)
                    if not boundary:
                        logging.error("Boundary not found in Content-Type header.")
                        break

                    for chunk in response.iter_content(chunk_size=8192):
                        buffer.extend(chunk)

                        while True:
                            frame, buffer = self._extract_frame(buffer, boundary)
                            if frame is None:
                                break
                            self._write_data(self._image_to_rgb24(bytes(frame)))
                else:
                    logging.error(f"Received unexpected status code {response.status_code}")
                    break
            except requests.RequestException as e:
                logging.exception(f"Exception occured during stream request")
                break


class LimaCamera(Camera):

    # Image modes in LImA. Supporting only Y8 and RGB24 for now.
    IMAGE_MODE_Y8 = 0
    IMAGE_MODE_RGB24 = 6

    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False, redis: str = None, redis_channel: str = None):
        super().__init__(device_uri, sleep_time, debug, redis, redis_channel)

        self._lima_tango_device = self._connect(self._device_uri)
        _, self._width, self._height, _ = self._get_image()
        self._sleep_time = sleep_time
        self._last_frame_number = -1

    def _connect(self, device_uri: str) -> "DeviceProxy":
        try: 
            from PyTango import DeviceProxy
        except ImportError:
            logging.error("PyTango is not installed. Please install it to use LImA cameras.")
            raise ImportError("PyTango is not installed. Please install it to use LImA cameras.")

        try:
            logging.info("Connecting to %s", device_uri)
            lima_tango_device = DeviceProxy(device_uri)
            lima_tango_device.ping()
        except Exception:
            logging.exception("")
            logging.info("Could not connect to %s, retrying ...", device_uri)
            sys.exit(-1)
        else:
            return lima_tango_device
        
    def _convert_to_rgb24(self, raw_image: bytearray, image_mode: int, width: int, height: int) -> bytearray:
        """Converts image's byte representation to RGB24 format, which is used by ffmpeg streaming process.

        Args:
            raw_image: image as bytes
            image_mode: LImA image mode
            width: width of image
            height: height of image
        Raises:
            NotImplementedError: unsupported image mode
        Returns:
            image as bytes in RGB24 format
        """
        if image_mode == self.IMAGE_MODE_Y8:
            raw_image = raw_image[:width * height]
            gray_img = Image.frombytes("L", (width, height), raw_image)
            rgb_img = gray_img.convert("RGB")
            return rgb_img.tobytes()
        elif image_mode == self.IMAGE_MODE_RGB24:
            # In RGB24 mode, we expect width*height*3 bytes.
            expected_bytes = width * height * 3
            return raw_image[:expected_bytes]
        else:
            logging.error(f"Unsupported image mode: {image_mode}")
            raise NotImplementedError(f"Conversion for image mode {image_mode} not implemented.")

    def _get_image(self) -> Tuple[bytearray, int, int, int]:
        """Gets a single image from the Tango device.
        
        Returns:
            raw_data: image as bytes
            width: width of image
            height: height of image
            frame_number: frame number of the image
        """
        img_data = self._lima_tango_device.video_last_image

        # Header format for `video_last_image` attribute in LImA.
        hfmt = ">IHHqiiHHHH"
        hsize = struct.calcsize(hfmt)
        header_fields = struct.unpack(hfmt, img_data[1][:hsize])
        (
            _magic_number,
            _version, 
            image_mode, 
            frame_number, 
            width, 
            height, 
            _endianness, 
            _header_size, 
            _padding, 
            _padding2
        ) = header_fields

        raw_data = self._convert_to_rgb24(
            raw_image=img_data[1][hsize:],
            image_mode=image_mode,
            width=width,
            height=height
            )
        
        return raw_data, width, height, frame_number

    def _poll_once(self) -> None:
        frame_number = self._lima_tango_device.video_last_image_counter

        if self._last_frame_number != frame_number:
            raw_data, width, height, frame_number = self._get_image()
            self._raw_data = raw_data

            self._write_data(self._raw_data)
            self._last_frame_number = frame_number

            if self._redis:
                frame_dict = {
                    "data": base64.b64encode(self._raw_data).decode('utf-8'),
                    "size": (width, height),
                    "time": datetime.now().strftime("%H:%M:%S.%f"),
                    "frame_number": self._last_frame_number,
                }
                self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))

        time.sleep(self._sleep_time / 2)


class RedisCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False, out_redis: str = None, out_redis_channel: str = None, in_redis_channel: str = 'frames'):
        super().__init__(device_uri, sleep_time, debug, out_redis, out_redis_channel)
        # for this camera in_redis_... is for the input and redis_... as usual for output
        self._in_redis_client = self._connect(self._device_uri)
        self._last_frame_number = -1
        self._in_redis_channel = in_redis_channel
        self._last_redis_drop_log_ts = 0.0
        self._md3_cam_prefix = (
            in_redis_channel.rsplit(":", 1)[0]
            if isinstance(in_redis_channel, str) and in_redis_channel.endswith(":RAW")
            else None
        )
        self._md3_header_format = "<HiiHHQH"
        self._md3_header_size = struct.calcsize(self._md3_header_format)
        self._ensure_md3_video_live()
        self._set_size()

    def _log_redis_drop(self, context: str, exc: Exception) -> None:
        if self._debug:
            logging.exception("Redis connection dropped %s", context)
        else:
            # Rate-limit warnings to avoid log spam during reconnect loops.
            now = time.time()
            if now - self._last_redis_drop_log_ts >= 5:
                logging.warning("Redis connection dropped %s: %s", context, exc)
                self._last_redis_drop_log_ts = now

    def _ensure_md3_video_live(self) -> None:
        """MD3 servers typically require setting `video_live=1` to start streaming.

        If the input channel looks like `<cam>:RAW`, we publish to `<cam>:SET:video_live`.
        """
        if not self._md3_cam_prefix:
            return

        cmd_channel = f"{self._md3_cam_prefix}:SET:video_live"
        attr_channel = f"{self._md3_cam_prefix}:ATTR:video_live"
        try:
            self._in_redis_client.ping()
            attr_pubsub = self._in_redis_client.pubsub()
            attr_pubsub.subscribe(attr_channel)
            self._in_redis_client.publish(cmd_channel, 1)

            start = time.perf_counter()
            while time.perf_counter() - start < 2:
                msg = attr_pubsub.get_message(timeout=0.2)
                if msg and msg.get("type") == "message":
                    data = msg.get("data")
                    if data == b"OK" or data == "OK":
                        break
            try:
                attr_pubsub.close()
            except Exception:
                pass
        except Exception:
            # Best-effort only; MD3 servers may not expose the control channels.
            if self._debug:
                logging.exception("Failed to set MD3 video_live")

    def _decode_encoded_image_to_rgb24(self, image: bytes) -> Tuple[bytes, int, int]:
        image_array = np.frombuffer(image, dtype=np.uint8)
        frame = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
        if frame is None:
            raise ValueError("Could not decode image bytes")
        height, width = frame.shape[:2]
        rgb24 = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB).tobytes()
        return rgb24, int(width), int(height)

    def _set_size(self):
        # the size is send via redis, hence we get the information from there
        backoff_s = 0.5
        while True:
            pubsub = None
            try:
                self._ensure_md3_video_live()
                pubsub = self._in_redis_client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self._in_redis_channel)
                while True:
                    try:
                        message = pubsub.get_message(timeout=1.0)
                    except TypeError:
                        message = pubsub.get_message()
                        time.sleep(0.01)

                    if not message or message.get("type") != "message":
                        continue

                    data = message.get("data")
                    if data is None or isinstance(data, int):
                        continue

                    # 1) Preferred format: JSON string with {"data": <base64>, "size": (w,h) or (h,w), ...}
                    try:
                        if isinstance(data, str):
                            frame = json.loads(data)
                        elif isinstance(data, (bytes, bytearray)):
                            stripped = bytes(data).lstrip()
                            if stripped.startswith((b"{", b"[")):
                                frame = json.loads(stripped)
                            else:
                                frame = None
                        else:
                            frame = None
                    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                        frame = None

                    if isinstance(frame, dict) and "size" in frame:
                        # Keep backward-compatible behaviour (existing code assumes size is (h, w)).
                        self._width = frame["size"][1]
                        self._height = frame["size"][0]
                        return

                    # 2) MD3/Arinax RAW format: binary header + raw bytes on channel like "bzoom:RAW"
                    if isinstance(data, (bytes, bytearray)) and len(data) >= self._md3_header_size:
                        try:
                            _, width, height, _, _, _, _ = struct.unpack(
                                self._md3_header_format, data[: self._md3_header_size]
                            )
                        except struct.error:
                            continue

                        if width > 0 and height > 0:
                            self._width = int(width)
                            self._height = int(height)
                            return

                    # 3) Raw encoded image bytes (JPEG/PNG/...) published directly (non-JSON)
                    if isinstance(data, (bytes, bytearray)):
                        try:
                            _rgb24, width, height = self._decode_encoded_image_to_rgb24(bytes(data))
                        except Exception:
                            continue
                        else:
                            if width > 0 and height > 0:
                                self._width = int(width)
                                self._height = int(height)
                                return

            except RedisConnectionError as exc:
                self._log_redis_drop("while probing size; reconnecting", exc)
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 1.5, 10)
                self._in_redis_client = self._connect(self._device_uri)
                continue
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

    def _connect(self, device_uri: str):
        parsed = urlparse(device_uri)
        if parsed.scheme and parsed.scheme != "redis":
            raise ValueError(f"Unsupported Redis URI scheme: {parsed.scheme}")

        host = parsed.hostname
        port = parsed.port
        username = parsed.username
        password = parsed.password
        db = int(parsed.path.lstrip("/") or 0) if parsed.scheme else 0

        # Fallback for bare host:port without scheme
        if host is None or port is None:
            endpoint = device_uri.replace("redis://", "")
            host_part, port_part = endpoint.split(":", 1)
            host = host_part
            port = int(port_part.split("/", 1)[0])

        # If parsing the db from URI path failed above due to bare host:port,
        # fall back to db=0.
        if not isinstance(db, int):
            db = 0

        return redis.StrictRedis(
            host=host,
            port=int(port),
            username=username,
            password=password,
            db=db,
            socket_keepalive=True,
            socket_connect_timeout=5,
            # Pub/Sub reads can block for a long time.
            socket_timeout=None,
            # Keep the TCP connection alive and detect stale connections.
            health_check_interval=30,
            retry_on_timeout=True,
            decode_responses=False,
        )

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output
        backoff_s = 0.5
        while True:
            pubsub = None
            try:
                self._ensure_md3_video_live()
                pubsub = self._in_redis_client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self._in_redis_channel)
                while True:
                    message = pubsub.get_message(timeout=1.0)
                    if not message:
                        continue

                    data = message.get("data")
                    if data is None or isinstance(data, int):
                        continue

                    # Try JSON frame first
                    try:
                        if isinstance(data, str):
                            frame = json.loads(data)
                        elif isinstance(data, (bytes, bytearray)):
                            stripped = bytes(data).lstrip()
                            if stripped.startswith((b"{", b"[")):
                                frame = json.loads(stripped)
                            else:
                                frame = None
                        else:
                            frame = None
                    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                        frame = None

                    if isinstance(frame, dict) and "data" in frame and "size" in frame:
                        self._last_frame_number += 1
                        if self._redis:
                            frame_dict = {
                                "data": frame["data"],
                                "size": frame["size"],
                                "time": datetime.now().strftime("%H:%M:%S.%f"),
                                "frame_number": self._last_frame_number,
                            }
                            self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))

                        raw_image_data = base64.b64decode(frame["data"])
                        self._write_data(self._image_to_rgb24(raw_image_data))
                        continue

                    # Fallback: MD3 binary RAW frame
                    if not isinstance(data, (bytes, bytearray)) or len(data) < self._md3_header_size:
                        # Final fallback: raw encoded image bytes (JPEG/PNG/...) or raw RGB24 without header
                        if isinstance(data, (bytes, bytearray)):
                            # If size is known, allow direct RGB24 payload (no header)
                            if self._width > 0 and self._height > 0:
                                expected_rgb = int(self._width) * int(self._height) * 3
                                if len(data) >= expected_rgb:
                                    rgb24 = bytes(data[:expected_rgb])
                                    self._last_frame_number += 1
                                    if self._redis:
                                        frame_dict = {
                                            "data": base64.b64encode(rgb24).decode("utf-8"),
                                            "size": (int(self._width), int(self._height)),
                                            "time": datetime.now().strftime("%H:%M:%S.%f"),
                                            "frame_number": self._last_frame_number,
                                        }
                                        self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))
                                    self._write_data(bytearray(rgb24))
                                    continue

                            # Otherwise try to decode it as an encoded image
                            try:
                                rgb24, width, height = self._decode_encoded_image_to_rgb24(bytes(data))
                            except Exception:
                                continue
                            else:
                                self._width = width
                                self._height = height
                                self._last_frame_number += 1
                                if self._redis:
                                    frame_dict = {
                                        "data": base64.b64encode(rgb24).decode("utf-8"),
                                        "size": (width, height),
                                        "time": datetime.now().strftime("%H:%M:%S.%f"),
                                        "frame_number": self._last_frame_number,
                                    }
                                    self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))
                                self._write_data(bytearray(rgb24))
                                continue

                        continue

                    try:
                        _, width, height, _, _, _, _ = struct.unpack(
                            self._md3_header_format, data[: self._md3_header_size]
                        )
                    except struct.error:
                        continue

                    width = int(width)
                    height = int(height)
                    payload = bytes(data[self._md3_header_size :])

                    if width <= 0 or height <= 0:
                        continue

                    expected_rgb = width * height * 3
                    expected_gray = width * height

                    if len(payload) >= expected_rgb:
                        rgb24 = payload[:expected_rgb]
                    elif len(payload) >= expected_gray:
                        gray = Image.frombytes("L", (width, height), payload[:expected_gray])
                        rgb24 = gray.convert("RGB").tobytes()
                    else:
                        continue

                    self._width = width
                    self._height = height
                    self._last_frame_number += 1

                    if self._redis:
                        frame_dict = {
                            "data": base64.b64encode(rgb24).decode("utf-8"),
                            "size": (width, height),
                            "time": datetime.now().strftime("%H:%M:%S.%f"),
                            "frame_number": self._last_frame_number,
                        }
                        self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))

                    self._write_data(bytearray(rgb24))

            except RedisConnectionError as exc:
                self._log_redis_drop("while listening; reconnecting", exc)
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 1.5, 10)
                self._in_redis_client = self._connect(self._device_uri)
                continue
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

class TestCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False, redis: str = None, redis_channel: str = None):
        super().__init__(device_uri, sleep_time, debug, redis, redis_channel)
        self._sleep_time = 0.05
        testimg_fpath = os.path.join(os.path.dirname(__file__), "fakeimg.jpg")
        self._im = Image.open(testimg_fpath, "r")

        self._raw_data = self._im.convert("RGB").tobytes()
        self._width, self._height = self._im.size
        self._last_frame_number = -1

    def _poll_once(self) -> None:
        self._write_data(bytearray(self._raw_data))
        
        self._last_frame_number += 1
        if self._redis:
            frame_dict = {
                "data": base64.b64encode(self._raw_data).decode('utf-8'),
                "size": self._im.size,
                "time": datetime.now().strftime("%H:%M:%S.%f"),
                "frame_number": self._last_frame_number,
            }
            self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))
        
        time.sleep(self._sleep_time)

class VideoTestCamera(Camera):
    def __init__(self, device_uri: str, sleep_time: float, debug: bool = False, redis: str = None, redis_channel: str = None):
        super().__init__(device_uri, sleep_time, debug, redis, redis_channel)
        self._sleep_time = 0.04
        # for your testvideo, please use an uncompressed video or mjpeg codec, 
        # otherwise, opencv might have issues with reading the frames.
        self._testvideo_fpath = os.path.join(os.path.dirname(__file__), "./test_video.avi")
        self._current = 0
        self._video_capture = cv2.VideoCapture(self._testvideo_fpath)
        self._set_video_dimensions()
        self._last_frame_number = -1

    def _poll_once(self) -> None:
        if not self._video_capture.isOpened():
            logging.error("Video capture is not opened.")
            return
        
        ret, frame = self._video_capture.read()
        if not ret:
            # End of video, loop back to the beginning
            self._video_capture.release()
            self._video_capture = cv2.VideoCapture(self._testvideo_fpath)
            ret, frame = self._video_capture.read()
            if not ret:
                logging.error("Failed to restart video capture.")
                return
            
        frame_pil = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        size = frame_pil.size        
        frame_bytes = frame_pil.tobytes()
        self._write_data(bytearray(frame_bytes))
        self._last_frame_number += 1
        if self._redis:
            frame_dict = {
                "data": base64.b64encode(frame_bytes).decode('utf-8'),
                "size": size,
                "time": datetime.now().strftime("%H:%M:%S.%f"),
                "frame_number": self._last_frame_number,
            }
            self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))
        
        time.sleep(self._sleep_time)

    def _set_video_dimensions(self):
        if not self._video_capture.isOpened():
            logging.error("Video capture is not opened.")
            return
        self._width = int(self._video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        self._height = int(self._video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))