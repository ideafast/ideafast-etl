import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

import requests
from dmpy.client import Dmpy
from dmpy.core.payloads import FileUploadPayload
from dmpy.core.utils import read_text_resource as dmpy_resource
from requests_toolbelt.multipart.encoder import (
    MultipartEncoder,
    MultipartEncoderMonitor,
)

from ideafast_etl.hooks.jwt import JwtHook


class DmpHook(JwtHook):
    """
    Hook for interfacing with the JWT API from ICL's DMP

    Based on JwtHook class (inherited) and Dmpy client class (copied)

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    default_conn_name = "dmp_default"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        """Init a JwtHook with overriden default connection name"""
        super().__init__(conn_id=conn_id)

    def _jwt_prepared_request(self) -> requests.PreparedRequest:
        """
        Return a Dmpy prepared JWT, overriding default JwtHook method

        To store long user/pass (e.g., public/private keys) in extras dict, and link to with
        "extra://extras_dict_key_to_long_value"
        """
        prefix = "extra://"
        user = (
            self.user
            if not self.user.startswith(prefix)
            else self.extras.get(self.user[len(prefix) :])
        )
        request = {
            "query": dmpy_resource("token.graphql"),
            "variables": {
                "pubkey": user,
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
        session = self.get_conn()

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

        session.headers = {
            "Content-Type": monitor.content_type,
            "Authorization": self._get_jwt_token(),
        }

        try:
            # Seconds to wait to establish connection with server
            connect = 4
            # Wait at most 5 minutes for server response between bytes sent
            # required as server timeout after uploading large files (>2GB)
            read = 60 * 5 + 2
            response = session.post(
                self.base_url,
                data=monitor,
                timeout=(connect, read),
                stream=True,
            )
            response.raise_for_status()

            # catch json decoder errors if API has not implemented HTTP errors fully
            try:
                json_response = response.json()
            except json.decoder.JSONDecodeError as e:
                logging.error(
                    "Unable to unpack HTTP body response in completing DMP upload: "
                    + str(e)
                )
                raise requests.HTTPError from e

            if "errors" in json_response:
                logging.error(f"Response was: {json_response}")
                raise Exception("UPLOAD_ERROR")

            logging.info(f"Uploaded {percent_uploaded}%")
            logging.debug(f"Response: {json_response}")

            return True
        except Exception:
            # TODO: consider raising this; now this goes unnotticed (except the return False)
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
