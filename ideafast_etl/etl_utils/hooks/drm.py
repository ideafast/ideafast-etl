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
        # don't create the hook here, as constructors are more frequently
        # called (in parsing DAGS) compared to using the hook with get_conn
        super().__init__()
        self._conn_id = conn_id
        self._retry = retry

        self._session: Optional[requests.Session] = None
        self._base_url = None
        self._extras: Dict[str, str] = {}
        self._login: Optional[Tuple[str, str]] = None

    def __enter__(self) -> BaseHook:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def get_jwt_token(self) -> str:
        """
        Check's the current token and refreses it if needed.
        Updates the connection details in 'extras' if changed
        """

        current_jwt = self._extras.get("jwt")

        if current_jwt:
            try:
                # only interested in the expiration date
                jwt.decode(
                    current_jwt,
                    options={"verify_signature": False, "verify_exp": True},
                )
                # not expired - ready to use
                return current_jwt
            except jwt.ExpiredSignatureError:
                pass

        # either expired or not present, request a new one
        response = requests.post(
            self._extras.get("token_host"), data={}, auth=self._login
        )
        response.raise_for_status()
        token: str = response.json().get("token")
        user_id: str = response.json().get("user_id")
        self._extras.update({"jwt": token, "user_id": user_id})

        # update Airflow connection to reflect change
        session = settings.Session()
        try:
            connection = (
                session.query(Connection)
                .filter(Connection.conn_id == self._conn_id)
                .one()
            )
            connection.extra = self._extras
            session.commit()
        finally:
            session.close()

        return token

    def get_conn(self) -> Tuple[requests.Session, str]:
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

        # return existing session, else create
        self._session = requests.Session() if self._session is None else self._session

        # get latest (or new) JWT token
        self._session.headers.update(
            {"Authorization": f"Bearer {self.get_jwt_token()}"}
        )

        return self._session, self._base_url

    def close(self) -> None:
        """Closes any active session."""
        if self._session:
            self._session.close()

        self._session = None
        self._base_url = None
        self._extras = {}
        self._login = None

    # API methods:

    def get_metadata(self) -> List[dict]:
        """
        GET all records (metadata) associated with the study site account
        This request is paginated and runs in multiple loops
        """
        session, base_url = self.get_conn()
        url = (
            base_url
            + f"/dreem/algorythm/restricted_list/{self._extras.get('user_id')}/record/"
        )

        results = []

        # url is None when pagination ends available
        while url:
            response = session.get(url)
            response.raise_for_status()
            result: dict = response.json()
            url = result.get("next")
            results.extend(result.get("results"))

        return results
