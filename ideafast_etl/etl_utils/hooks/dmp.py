import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dmpy.client import Dmpy
from dmpy.core.payloads import FileUploadPayload
from dmpy.core.utils import read_text_resource as dmpy_resource
from etl_utils.hooks.jwt import JwtHook


class DmpHook(JwtHook):
    """
    Hook for interfacing with the JWT API from ICL's DMP

    Based on JwtHook class (inherited) and Dmpy client class (referenced)

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

    def get_access_token(self) -> str:
        """Override Dmpy method with Hook method"""
        return self._get_jwt_token()

    def dmp_upload(self, dmp_dataset: str, path: Path) -> bool:
        """Upload a single file to the DMP. Uses the Dmpy.method"""
        patient_id, device_id, start, end = path.stem.split("-")

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

        # TODO: replace this odd overriding with own implementation of the upload
        return Dmpy.upload(self, payload)

    @staticmethod
    def rm_local_data(zip_path: Path) -> None:
        """Delete local folder based on zip path"""
        zip_path.unlink()
        shutil.rmtree(zip_path.with_suffix(""))

    @staticmethod
    def zip_folder(path: Path) -> Path:
        """Zip folder and return path"""
        return Path(shutil.make_archive(str(path), "zip", path))

    @staticmethod
    def weartime_in_ms(wear_time: str) -> int:
        """Convert DMP formatted weartime (20210101) to miliseconds."""
        return int(datetime.strptime(wear_time, "%Y%m%d").timestamp() * 1e3)
