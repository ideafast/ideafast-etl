from pathlib import Path
from typing import Iterator

import requests
from etl_utils.hooks.jwt import JwtHook


class DreemHook(JwtHook):
    """
    Hook for interfacing with the JWT REST APIs from Dreem

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def get_metadata(self, list_size: int = 30) -> Iterator[dict]:
        """
        GET all records (metadata) associated with the study site account

        This request is paginated and runs in multiple loops

        Parameters
        ----------
        list_size : int
            Size of the number of recordings to fetch from the API with each call
            Defaults to Dreem's default of 30
        """
        session = self.get_conn()

        url = (
            self.base_url
            + f"dreem/algorythm/restricted_list/{self.extras.get('user_id')}"
            + f"/record/?limit={list_size}"
        )

        # url is None when pagination ends available
        while url:
            response = session.get(url)
            response.raise_for_status()
            result: dict = response.json()
            url = result.get("next")
            yield from result.get("results")

    def download_file(self, file_ref: str, download_path: Path) -> bool:
        """
        Download file from Dreem servers

        Gets file location by querying Dreem API, then downloads from that
        location (file_url includes authentication)
        """
        session = self.get_conn()
        url = self.base_url + f"dreem/algorythm/record/{file_ref}/h5/"

        response = session.get(url)
        response.raise_for_status()
        result: dict = response.json()
        file_url = result.get("data_url", None)
        # NOTE: file_url may be empty if a file is unavailable:
        # (1): file is on dreem headband but not uploaded
        # (2): file is being processed by dreem's algorithms
        if not file_url:
            return False

        file_path = download_path / f"{file_ref}.h5"

        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            with open(file_path, "wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        output_file.write(chunk)

        return True
