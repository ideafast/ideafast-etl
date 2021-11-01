import json
from typing import Any, Dict, List, Optional, Tuple

import jwt
import requests
from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models import Connection


class DreemJwtHook(BaseHook):
    """
    Hook for REST APIs with JWT tokens especially for Dreem

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def __init__(self, conn_id: str, retry: int = 3) -> None:
        """
        Constructor. Does'nt initialise the Hook, as constructors
        are more frequently called (in parsing DAGS)
        """

        super().__init__()
        self._conn_id = conn_id
        self._retry = retry

        self._session: Optional[requests.Session] = None
        self._base_url = ""
        self._extras: Dict[str, str] = {}
        self._login: Optional[Tuple[str, str]] = None

    def __enter__(self) -> BaseHook:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _get_jwt_token(self) -> str:
        """
        Return's the latest JWT token, refreshes it if needed
        Updates the connection details in 'extras' if changed for reuse across tasks
        """
        current_jwt = self._extras.get("jwt")

        if current_jwt:
            try:
                # only interested in the expiration date
                jwt.decode(
                    current_jwt,
                    options={"verify_signature": False, "verify_exp": True},
                )
                return current_jwt
            except jwt.ExpiredSignatureError:
                pass

        # if expired (thown and passed exception), or no token - fetch one
        response = requests.post(
            self._extras.get("token_host"), data={}, auth=self._login
        )
        response.raise_for_status()
        self._extras.update(
            {
                "jwt": response.json().get("token"),
                "user_id": response.json().get("user_id"),  # needed for Dreem API
            }
        )

        # update Airflow connection to reflect change
        session = settings.Session()
        try:
            connection = (
                session.query(Connection)
                .filter(Connection.conn_id == self._conn_id)
                .one()
            )
            # use json.dumps for double quotes "" (needed for Airflow)
            connection.extra = json.dumps(self._extras)
            session.commit()
        finally:
            session.close()

        return self._extras.get("token")

    def get_conn(self) -> requests.Session:
        """
        Returns the connection used by the hook for querying data.
        Should in principle not be used directly.
        """

        # get initial details from Airflow Connections is no session exists
        if self._session is None:
            config = self.get_connection(self._conn_id)
            self._extras = config.extra_dejson

            if not config.host or not self._extras.get("token_host"):
                raise ValueError(
                    f"No (token)host specified in connection {self._conn_id}"
                )

            self._login = (config.login, config.password)
            self._base_url = config.host

        # return existing session, else create
        self._session = requests.Session() if self._session is None else self._session

        # ensure working JWT token
        self._session.headers.update(
            {"Authorization": f"Bearer {self._get_jwt_token()}"}
        )

        return self._session

    def close(self) -> None:
        """Closes any active session."""
        if self._session:
            self._session.close()

        self._session = None
        self._base_url = None
        self._extras = {}
        self._login = None

    # API methods:

    def get_metadata(self, list_size: int = 30) -> List[dict]:
        """
        GET all records (metadata) associated with the study site account
        This request is paginated and runs in multiple loops

        Parameters
        ----------
        limit : bool

        list_size : int
            Size of the number of recordings to fetch from the API. Defaults
            to Dreem's default of 30
        """
        session = self.get_conn()

        url = (
            self._base_url
            + f"dreem/algorythm/restricted_list/{self._extras.get('user_id')}"
            + f"/record/?limit={list_size}"
        )

        results = []

        # url is None when pagination ends available
        while url:
            response = session.get(url)
            print(response.request.headers)
            response.raise_for_status()
            result: dict = response.json()
            url = result.get("next")
            results.extend(result.get("results"))

        return results
