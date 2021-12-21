import json
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

    # FIXME: remove variable once live connection
    CURRENT_DIR = Path(__file__).parent

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

        # response = session.get(url)
        # response.raise_for_status()
        # result: dict = response.json()
        # FIXME: use active url above when LIVE
        with open(
            self.CURRENT_DIR.parent / "dummy/test_wks_participants.json", "r"
        ) as f:
            result: dict = json.load(f)

            return [k for k in result.keys()]

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

        # response = session.get(url)
        # response.raise_for_status()
        # result: list = response.json()
        # FIXME: use active url above when LIVE
        with open(self.CURRENT_DIR.parent / "dummy/test_wks_timestamps.json", "r") as f:
            # use line below to test that it picks up new records if more timestamps are found
            # with open(
            #     self.CURRENT_DIR.parent / "dummy/test_wks_timestamps_found_more.json", "r"
            # ) as f:
            result: dict = json.load(f)

            days = defaultdict(list)
            for timestamp in result:
                in_seconds = int(timestamp / 1e3)
                day = datetime.fromtimestamp(in_seconds).strftime(self.date_format)
                days[day].append(in_seconds)

            return days

    def download_file(
        self, participant_id: str, date: str, download_path: Path
    ) -> bool:
        """Download file from Wildkeys servers"""
        session = self.get_conn()
        participant = participant_id.replace("-", "")
        url = (
            self.base_url
            + f"participants/{participant}"
            + f"?from={self.start_of_day(date)}&to={self.end_of_day(date)}"
        )

        file_path = download_path / f"{participant}-{date}.json"

        # with session.get(url, stream=True) as response:
        # response.raise_for_status()
        # total_size = int(response.headers.get("content-length"))
        # FIXME: use active url above when LIVE
        with open(
            self.CURRENT_DIR.parent / "dummy/test_wks_download.json", "r"
        ) as response:
            total_size = 1065  # simulate lines as chunks
            total_down, percent_down = 0, 0

            with open(file_path, "wb") as output_file:
                # for chunk in response.iter_content(chunk_size=1024):
                # FIXME: use iter_content from response.json() once LIVE
                for fake_chunk in response.readline():
                    # FIXME: remove byte conversion when LIVE
                    chunk = bytes(fake_chunk, "utf-8")
                    if chunk:
                        output_file.write(chunk)
                        # total_down += len(chunk)
                        # FIXME: use chunk length when LIVE
                        total_down += 1
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
