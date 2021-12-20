import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ideafast_etl.hooks.jwt import JwtHook


class WildkeysHook(JwtHook):
    """
    Hook for interfacing with the JWT REST APIs from Wildkeys

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    date_format = "%Y%m%d"
    default_conn_name = "wks_default"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        """Init a JwtHook with overriden default connection name"""
        super().__init__(conn_id=conn_id)

    def _get_jwt_token(self) -> str:
        """FIXME: TEMPORARY OVERRIDE FOR TESTING"""
        return "all_good"

    def get_participants(self) -> List[str]:
        """GET all participants"""
        session = self.get_conn()

        url = self.base_url + "/participants/list"

        response = session.get(url)
        response.raise_for_status()
        result: dict = response.json()

        return [k for k in result.get("participants").keys()]

    def get_metadata(self, participant: str) -> Dict[str, list]:
        """
        GET a count of recordings per day of a participant

        Parameters
        ----------
        participants : str
            a participant ID to query the API for
        """
        session = self.get_conn()

        url = self.base_url + f"/participants/{participant}/summary"

        response = session.get(url)
        response.raise_for_status()
        result: list = response.json()

        days = defaultdict(list)
        for timestamp in result:
            day = datetime.fromtimestamp(timestamp).strftime(self.date_format)
            days[day].append(timestamp)

        return days

    def download_file(self, file_ref: str, download_path: Path) -> bool:
        """Download file from Wildkeys servers"""
        session = self.get_conn()
        participant_id, date = file_ref.split("-")
        url = (
            self.base_url
            + f"participants/{participant_id}"
            + f"?from={self.start_of_day(date)}&to={self.end_of_day(date)}"
        )

        file_path = download_path / f"{file_ref}.json"

        with session.get(url, stream=True) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("content-length"))
            total_down, percent_down = 0, 0

            with open(file_path, "wb") as output_file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        output_file.write(chunk)
                        total_down += len(chunk)
                    if (
                        chunk
                        and (status := int((total_down / total_size) * 100))
                        > percent_down + 10
                    ):
                        percent_down = round(status, -1)
                        logging.info(f"{percent_down}% Downloaded")

                logging.info("100% Downloaded")

        return True

    @classmethod
    def start_of_day(cls, day: str) -> int:
        """Normalise a daytime to 00:00:00 for unix timestamp"""
        start = datetime.strptime(day, cls.date_format)
        return int(
            start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1e3
        )

    @classmethod
    def end_of_day(cls, day: str) -> int:
        """Normalise a daytime to 23:59:59 for unix timestamp"""
        end = datetime.strptime(day, cls.date_format)
        return int(
            end.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp()
            * 1e3
        )
