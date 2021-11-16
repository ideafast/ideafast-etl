import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dmpy.client import Dmpy
from dmpy.core.payloads import FileUploadPayload
from dmpy.core.utils import read_text_resource as dmpy_resource
from etl_utils.hooks.jwt import JwtHook
from requests_toolbelt.multipart.encoder import (
    MultipartEncoder,
    MultipartEncoderMonitor,
)


class DmpHook(JwtHook):
    """
    Hook for interfacing with the JWT API from ICL's DMP

    Based on JwtHook class (inherited) and Dmpy client class (copied)

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def __init__(self, **kwds: Any) -> None:
        """Construct, but not initialise, the Hook."""
        super().__init__(**kwds)

        # required for Dmpy.upload method
        self.url = self.base_url

    def _jwt_prepared_request(self) -> requests.PreparedRequest:
        """Return a Dmpy prepared JWT, overriding default JwtHook method"""
        request = {
            "query": dmpy_resource("token.graphql"),
            "variables": {
                "pubkey": self.user,
                "signature": self.passw,
            },
        }
        return requests.Request("POST", self.jwt_url, json=request).prepare()

    def upload(self, dmp_dataset: str, path: str) -> bool:
        """
        Upload a single file to the DMP. Based on the Dmpy.method

        Currently uses a mix of copied code and module methods, should be aligned
        with - potentially - an update on the dmpy module
        """
        patient_id, device_id, start, end = Path(path).stem.split("-")

        checksum = Dmpy.checksum(path)
        start_wear = self.weartime_in_ms(start)
        end_wear = self.weartime_in_ms(end)

        payload = FileUploadPayload(
            path,
            patient_id,
            device_id,
            start_wear,
            end_wear,
            checksum,
            dmp_dataset,
        )

        encoder = MultipartEncoder(
            {
                "operations": payload.operations(),
                "map": json.dumps({"fileName": ["variables.file"]}),
                "fileName": (
                    payload.path.name,
                    open(
                        payload.path,
                        "rb",
                    ),
                    "application/octet-stream",
                ),
            }
        )

        percent_uploaded = 0

        def log_progress(monitor: MultipartEncoderMonitor) -> None:
            """Log data transfer progress when 10% of file uploaded."""
            # Gain access to the variable in the outter scope
            nonlocal percent_uploaded

            bytes_sent = monitor.bytes_read
            upload_percent = int(bytes_sent / monitor.len * 100)
            blocksize = 8192

            if (
                bytes_sent == blocksize
                or upload_percent % 10 == 0
                and percent_uploaded != upload_percent
            ):
                logging.info(f"{upload_percent}% Uploaded | {bytes_sent} Bytes Sent")
                percent_uploaded = upload_percent

        monitor = MultipartEncoderMonitor(encoder, log_progress)

        headers = {
            "Content-Type": monitor.content_type,
            "Authorization": self._get_jwt_token(),
        }

        try:
            # Seconds to wait to establish connection with server
            connect = 4
            # Wait at most 5 minutes for server response between bytes sent
            # required as server timeout after uploading large files (>2GB)
            read = 60 * 5 + 2
            response = requests.post(
                self.url,
                data=monitor,
                headers=headers,
                timeout=(connect, read),
                stream=True,
            )
            response.raise_for_status()

            json_response = response.json()

            if "errors" in json_response:
                logging.error(f"Response was: {json_response}")
                raise Exception("UPLOAD_ERROR")

            logging.info(f"Uploaded {percent_uploaded}%")
            logging.debug(f"Response: {json_response}")

            return True
        except Exception:
            logging.error("Exception:", exc_info=True)
        return False

    @staticmethod
    def rm_local_data(zip_path: Path) -> None:
        """Delete local folder based on zip path"""
        if zip_path.exists():
            zip_path.unlink()
        folder_path = zip_path.with_suffix("")
        if folder_path.exists():
            shutil.rmtree(zip_path.with_suffix(""))
        logging.debug(f"Cleaned up {zip_path.stem}")

    @staticmethod
    def zip_folder(path: Path) -> Path:
        """Zip folder and return path"""
        zip_path = Path(shutil.make_archive(str(path), "zip", path))
        logging.debug(f"Created {zip_path.name}")
        return zip_path

    @staticmethod
    def weartime_in_ms(wear_time: str) -> int:
        """Convert DMP formatted weartime (20210101) to miliseconds."""
        return int(datetime.strptime(wear_time, "%Y%m%d").timestamp() * 1e3)
