import time
import logging
import struct
import sys
import os
import io
import subprocess
import multiprocessing
import multiprocessing.queues
import requests
import redis
import json
import base64
from datetime import datetime
import cv2
import numpy as np

from contextlib import contextmanager

from typing import Union, IO, Tuple, Optional, Iterator, Any

from PIL import Image

from requests.auth import HTTPBasicAuth, HTTPDigestAuth
from video_streamer.core.config import AuthenticationConfiguration

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _rust_camera_bin_path() -> str:
    # Allow overriding the binary path (useful for packaging / CI).
    override = os.environ.get("VIDEO_STREAMER_RUST_CAMERA_BIN")
    if override:
        return override

    # Default: cargo build output
    bin_name = "video_streamer_camera"
    return os.path.join(_repo_root(), "rust", "target", "release", bin_name)


def _ensure_rust_camera_binary() -> str:
    bin_path = _rust_camera_bin_path()
    if os.path.exists(bin_path) and os.access(bin_path, os.X_OK):
        return bin_path

    manifest = os.path.join(_repo_root(), "rust", "Cargo.toml")
    logger.info("Rust camera binary not found; building via cargo: %s", manifest)

    try:
        subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", manifest],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "cargo not found in PATH; build rust/video_streamer_camera first "
            "or set VIDEO_STREAMER_RUST_CAMERA_BIN"
        ) from e

    if not (os.path.exists(bin_path) and os.access(bin_path, os.X_OK)):
        raise RuntimeError(f"Rust camera binary not executable: {bin_path}")

    return bin_path


def _read_exact(stream: Any, n: int) -> Optional[bytes]:
    """Read exactly n bytes from a file-like object; return None on EOF."""
    buf = bytearray(n)
    view = memoryview(buf)
    pos = 0
    while pos < n:
        chunk = stream.read(n - pos)
        if not chunk:
            return None
        view[pos : pos + len(chunk)] = chunk
        pos += len(chunk)
    return bytes(buf)

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
                if self._redis_channel is not None:
                    self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))

        time.sleep(self._sleep_time / 2)


class RedisCamera(Camera):
    def __init__(
        self,
        device_uri: str,
        sleep_time: float,
        debug: bool = False,
        out_redis: Optional[str] = None,
        out_redis_channel: Optional[str] = None,
        in_redis_channel: str = "frames",
    ):
        if out_redis is not None:
            raise NotImplementedError(
                "Out redis not currently implemented for RedisCamera."
            )

        super().__init__(device_uri, sleep_time, debug, None, None)
        # IMPORTANT (multiprocessing): do not keep Redis sockets open on the camera
        # instance at construction time, since the camera object is created in the
        # parent process and then used in a child process.
        self._last_frame_number = -1
        self._in_redis_channel = in_redis_channel

        self._md3_header_format = "<HiiHHQH"
        self._md3_header_size = struct.calcsize(self._md3_header_format)
        self._set_size()

    @contextmanager
    def _managed_pubsub(self, device_uri: str):
        client = self._connect(device_uri)
        pubsub = client.pubsub()
        try:
            yield pubsub
        finally:
            try:
                pubsub.close()
            except Exception:
                pass
            try:
                client.close()  # type: ignore[attr-defined]
            except Exception:
                try:
                    client.connection_pool.disconnect(inuse_connections=True)
                except Exception:
                    pass

    def _set_size(self) -> None:
        # The size is sent via Redis; we read a single message to discover it.
        with self._managed_pubsub(self._device_uri) as pubsub:
            pubsub.subscribe(self._in_redis_channel)
            while True:
                message = pubsub.get_message()
                if message and message.get("type") == "message":
                    _, width, height, _, _, _, _ = struct.unpack(
                        self._md3_header_format,
                        message["data"][: self._md3_header_size],
                    )
                    self._width, self._height = width, height
                    return
                time.sleep(0.001)

    def _connect(self, device_uri: str):
        host, port = device_uri.replace('redis://', '').split(':')
        port = port.split('/')[0]
        return redis.StrictRedis(host=host, port=int(port))

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output

        # Heavy lifting (redis pubsub + gray->rgb conversion) is done in Rust.
        # This Python process just forwards fixed-size rgb24 frames into ffmpeg.
        bin_path = _ensure_rust_camera_binary()
        frame_size = int(self._width * self._height * 3)
        if frame_size <= 0:
            raise RuntimeError(f"Invalid RedisCamera frame size: {self._width}x{self._height}")

        proc = subprocess.Popen(
            [
                bin_path,
                "redis",
                "--uri",
                self._device_uri,
                "--channel",
                self._in_redis_channel,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL if not self._debug else None,
            bufsize=0,
        )

        assert proc.stdout is not None

        try:
            while True:
                frame = _read_exact(proc.stdout, frame_size)
                if frame is None:
                    break

                self._last_frame_number += 1
                self._write_data(bytearray(frame))
        except KeyboardInterrupt:
            sys.exit(0)
        except BrokenPipeError:
            logger.info(
                f"RedisCamera stream ended (broken pipe). Frame number: {self._last_frame_number}"
            )
            sys.exit(0)
        except Exception:
            logger.exception(
                "RedisCamera encountered an error during streaming."
                f" Frame number: {self._last_frame_number}"
            )
        finally:
            try:
                proc.terminate()
            except Exception:
                pass
                
                
                #raw_image_data = base64.b64decode(raw)                
                # self._write_data(self._image_to_rgb24(raw))

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
        # Kept for backwards compatibility; the fast path is in poll_image().
        self._write_data(bytearray(self._raw_data))
        self._last_frame_number += 1
        time.sleep(self._sleep_time)

    def poll_image(self, output: Union[IO, multiprocessing.queues.Queue]) -> None:
        self._output = output
        if self._redis:
            host, port = self._redis.split(':')
            self._redis_client = redis.StrictRedis(host=host, port=int(port))

        frame_size = int(self._width * self._height * 3)
        if frame_size <= 0:
            raise RuntimeError(f"Invalid TestCamera frame size: {self._width}x{self._height}")

        bin_path = _ensure_rust_camera_binary()
        image_path = os.path.join(os.path.dirname(__file__), "fakeimg.jpg")

        proc = subprocess.Popen(
            [
                bin_path,
                "test",
                "--image-path",
                image_path,
                "--sleep-ms",
                str(int(self._sleep_time * 1000)),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL if not self._debug else None,
            bufsize=0,
        )

        assert proc.stdout is not None

        try:
            while True:
                frame = _read_exact(proc.stdout, frame_size)
                if frame is None:
                    break

                self._last_frame_number += 1
                self._write_data(bytearray(frame))

                if self._redis and self._redis_channel is not None:
                    frame_dict = {
                        "data": base64.b64encode(frame).decode("utf-8"),
                        "size": (self._width, self._height),
                        "time": datetime.now().strftime("%H:%M:%S.%f"),
                        "frame_number": self._last_frame_number,
                    }
                    self._redis_client.publish(self._redis_channel, json.dumps(frame_dict))
        except KeyboardInterrupt:
            sys.exit(0)
        except BrokenPipeError:
            sys.exit(0)
        finally:
            try:
                proc.terminate()
            except Exception:
                pass

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