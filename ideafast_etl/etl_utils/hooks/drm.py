from typing import Iterator

from etl_utils.hooks.jwt import JwtHook


class DreemJwtHook(JwtHook):
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
