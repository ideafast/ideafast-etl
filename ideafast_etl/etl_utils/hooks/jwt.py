import json
from functools import reduce
from operator import getitem
from typing import Any, Optional

import jwt
import requests
from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models import Connection


class JwtHook(BaseHook):
    """
    Hook for REST APIs with JWT tokens

    Requires a json dict payload from a jwt token endpoint
        using basicauth with the provided user/pass credentials
    Requires a 'jwt_url' in the connections.extras field.
    Requires a 'jwt_json_path' in the connections.extra field, indicating
        the path to the token from the jwt_url response (dot notation)
    Stores a JWT token as 'jwt_token' in the connections.extras field.
    Retains other items in the connections.extras field as long as no overlap

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Dreem API
    """

    def __init__(self, conn_id: str, retry: int = 3) -> None:
        """Construct, but not initialise, the Hook."""
        super().__init__()
        self.conn_id = conn_id
        self.retry = retry

        self.session: Optional[requests.Session] = None
        self.base_url: Optional[str] = None
        self.user: Optional[str] = None
        self.passw: Optional[str] = None
        self.jwt_token: Optional[str] = None
        self.jwt_token_path: Optional[str] = None
        self.jwt_url: Optional[str] = None

    def __enter__(self) -> BaseHook:
        """Return the Hook when used in context management"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the Hook when used in context management"""
        self.close()

    def _find_jwt_token(self, jwt_path: str, jwt_payload: dict) -> str:
        """Get the token from the jwt_payload following the dot notated path"""
        return str(reduce(getitem, jwt_path.split("."), jwt_payload))

    def _get_jwt_token(self) -> str:
        """
        Return the latest JWT token, refreshes it if needed

        Updates the connection details in 'extras' if changed for reuse across tasks
        """
        if self.jwt_token:
            try:
                # only interested in the expiration date
                jwt.decode(
                    self.jwt_token,
                    options={"verify_signature": False, "verify_exp": True},
                )
                return self.jwt_token
            except jwt.ExpiredSignatureError:
                pass

        # if expired (thown and passed exception), or no token - fetch one
        response = requests.post(self.jwt_url, data={}, auth=(self.user, self.passw))
        response.raise_for_status()
        self.jwt_token = self._find_jwt_token(
            jwt_path=self.jwt_token_path, jwt_payload=response.json()
        )

        # update Airflow connection to reflect change
        session = settings.Session()
        try:
            connection = (
                session.query(Connection)
                .filter(Connection.conn_id == self.conn_id)
                .one()
            )
            # copy, update and push the jwt token to the Connection storage
            new_extra: dict = json.loads(connection.extra)
            new_extra.update({"jwt_token": self.jwt_token})
            connection.extra = json.dumps(new_extra)
            session.commit()
        finally:
            session.close()

        return self.jwt_token

    def get_conn(self) -> requests.Session:
        """Get the connection used by the hook for querying data."""
        if self.session is None:
            config = self.get_connection(self.conn_id)
            extras = config.extra_dejson

            if not config.host:
                raise ValueError(f"No host specified in connection {self.conn_id}")

            if not extras.get("jwt_url"):
                raise ValueError(
                    f"No url (jwt_url) for getting jwt token provided in connection {self.conn_id}"
                )

            if not extras.get("jwt_token_path"):
                raise ValueError(
                    "No path.to.token (jwt_token_path) for jwt_url payload provided "
                    + f"in connection {self.conn_id}"
                )

            self.base_url = config.host
            self.user = config.login
            self.passw = config.password
            self.jwt_token = extras.pop("jwt_token", None)
            self.jwt_token_path = extras.pop("jwt_token_path")
            self.jwt_url = extras.pop("jwt_url")
            # store other extras in self.extras for use in subclasses if needed
            self.extras = extras

        # return existing session, else create
        self.session = requests.Session() if self.session is None else self.session

        # ensure working JWT token
        self.session.headers.update(
            {"Authorization": f"Bearer {self._get_jwt_token()}"}
        )

        return self.session

    def close(self) -> None:
        """Close any active session."""
        if self.session:
            self.session.close()

        self.session = None
        self.base_url = None
        self.user = None
        self.passw = None
        self.jwt_token = None
        self.jwt_token_path = None
        self.jwt_url = None