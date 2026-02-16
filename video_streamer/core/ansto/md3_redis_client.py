import struct
from typing import Union

import numpy as np
import redis
import time
from PIL import (
    Image,
    ImageOps,
)
from redis.exceptions import ConnectionError


INT, FLOAT, STRING = 0, 1, 2
IS_RANGE_IDX = 1
parsers = {INT: int, FLOAT: float, STRING: None}
MD3_CAMERA_TIMEOUT = 0.2  # seconds


class NoFrameFoundError(TimeoutError):
    """Raised when no image frame is received from the MD3 redis server"""


class MD3RedisClient(redis.Redis):
    """
    Based on Arinax code.
    This client connects to the MD3 redis server to get images
    and set/get camera attributes.
    """

    attributes = {
        "video_live": [INT, False],
    }
    # ------------------
    header_format = "<HiiHHQH"
    header_size = struct.calcsize(header_format)

    def __init__(self, args):
        redis.Redis.__init__(self, host=args["host"], port=args["port"], db=0)
        self._cameras = [args["hybrid"], args["first"], args["second"]]
        self._attr_pubsub = None
        self._img_pubsub = None

        self.width: int = None
        self.height: int = None
        self.depth: int = None

        img = self.get_frame()
        try:
            self.width, self.height, self.depth = np.array(img).shape
        except ValueError:
            self.depth = None
            self.width, self.height = np.array(img).shape

    def _initialise_client(self) -> bool:
        """
        This function initializes the client (ping to check connection, read cameras
        zoom levels & prepare image and attribute listeners)

        Returns
        -------
        bool
            A boolean indicating if the client was correctly initialized
        """
        # Ping
        try:
            self.ping()
        except ConnectionError as e:
            msg = f"Could not connect to MD3 redis server: {e}"
            print(msg)
            raise ConnectionError(msg) from e
        # Create clients to listen to attribute and image channels
        self._attr_pubsub = self.pubsub()
        self._img_pubsub = self.pubsub()
        # Start listening on attributes channel
        self._attr_pubsub.psubscribe("*:ATTR:*")
        while self._attr_pubsub.get_message() is None:
            continue
        try:
            self._img_pubsub.subscribe(self._cameras[0] + ":RAW")
        except ConnectionError as ex:
            raise ConnectionError(f"Could not subscribe to image channel: {ex}") from ex
        return True

    def _read_attribute(
        self, name: str, cam_idx: int = 0
    ) -> Union[str, int, float, list]:
        """
        This function reads the value of an attribute of one of the cameras of the video server.

        Parameters
        ----------
        name : str
            attribute's name
        cam_idx : int, optional
            index of the camera which attribute is read

        Returns
        -------
        Union[str, int, float, list]
            Attribute's value
        """
        attr_name = name if cam_idx == 0 else self._cameras[cam_idx] + "::" + name
        [attr_type, is_range] = MD3RedisClient.attributes[name][: IS_RANGE_IDX + 1]
        parser = parsers[attr_type]
        if not is_range:
            res = self.get(attr_name)
            if res is not None:
                res = res.decode("utf-8")
                if parser is not None:
                    res = parser(res)
        else:
            res = self.lrange(attr_name, 0, -1)
            if res is not None:
                res = [res_el.decode("utf-8") for res_el in res]
                if parser is not None:
                    res = [parser(res_el) for res_el in res]
        return res

    def _write_attribute(
        self, name: str, value: Union[str, int, float], cam_idx: int = 0
    ) -> bool:
        """
        This function sets a specific camera's attribute to a desired value.

        Parameters
        ----------
        name : str
            Name of the attribute
        value : Union[str, int, float]
            Value to set to the attribute
        cam_idx : int, optional
            Index of the camera which attribute is modified, by default 0

        Returns
        -------
        bool
            True if write successful, false otherwise
        """
        cmd_channel = self._cameras[cam_idx] + ":SET:" + name
        attr_channel = self._cameras[cam_idx] + ":ATTR:" + name
        rep = None
        self.publish(cmd_channel, value)
        start_time = time.perf_counter()
        while (rep is None or rep["channel"].decode("utf-8") != attr_channel) and (
            time.perf_counter() - start_time < 2
        ):
            try:
                rep = self._attr_pubsub.get_message()
            except ConnectionError as e:
                msg = f"MD3 redis connection error while getting attribute message: {e}"
                print(msg)
                raise ConnectionError(msg) from e
        return (
            True if rep is not None and rep["data"].decode("utf-8") == "OK" else False
        )

    def _poll_image(self, timeout: float = 0.2) -> tuple[bytes, int, int]:
        """
        This function is used to retrieve one image from the Redis video server.

        Parameters
        ----------
        timeout : float, optional
            Time in seconds to wait for an image before raising NoFrameFoundError,
            by default 0.5

        Returns
        -------
        tuple[bytes, int, int]
            The raw data in bytes format, width, and height

        Raises
        -------
        NoFrameFoundError
            If no frame is found within the timeout period
        """
        msg = None
        start = time.perf_counter()
        while True:
            if time.perf_counter() - start > timeout:
                raise NoFrameFoundError(
                    f"No frame found within {timeout}s while waiting for image message"
                )
            try:
                msg = self._img_pubsub.get_message()
            except ConnectionError as e:
                msg = f"MD3 redis connection error while getting image message: {e}"
                print(msg)
                raise ConnectionError(msg) from e
            if msg is not None and not isinstance(msg["data"], int):
                break
            time.sleep(0.001)
        _, width, height, _, _, _, _ = struct.unpack(
            MD3RedisClient.header_format, msg["data"][: MD3RedisClient.header_size]
        )
        raw = msg["data"][MD3RedisClient.header_size :]
        return raw, width, height

    def get_frame(self) -> Image.Image:
        """
        Gets a frame from the MD3 redis server.

        Returns
        -------
        Image
            An image object
        """
        if not self._initialise_client():
            print("MD3 redis client could not be initialized")
            return

        if self._read_attribute("video_live") != 1:
            self._write_attribute("video_live", 1)

        raw, self.width, self.height = self._poll_image(MD3_CAMERA_TIMEOUT)
        try:
            img = Image.frombytes(mode="RGB", size=(self.width, self.height), data=raw)
        except ValueError:
            # black and white image
            img = Image.frombytes(mode="L", size=(self.width, self.height), data=raw)

        # NOTE: The MD3 redis server returns mirrored images, therefore we mirror them back
        # this is a test to see if docker cp worked
        return ImageOps.mirror(img)

if __name__ == "__main__":
    default_args = {
        "host": "host",
        "port": 6379,
        "hybrid": "bzoom",
        "first": "acA2500-x5",
        "second": "acA2440-x30",
    }
    cam = MD3RedisClient(default_args)
    print(np.array(cam.get_frame()))
    print("height:", cam.height)
    print("width:", cam.width)
