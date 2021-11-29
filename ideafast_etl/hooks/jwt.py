import json
import logging
import re
from functools import reduce
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

    By default, this hook retrieves a JWT token through basicAuth. If this
    is different for your case, create a subclass and override the prepped_jwt_request.

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the JWT HTTPS API
    """

    default_conn_name = "jwt_default"

    def __init__(self, conn_id: str = default_conn_name, retry: int = 3) -> None:
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

        def get_despite_none(payload: Optional[dict], key: str) -> Any:
            """Try to get value from dict, even if dict is None"""
            if not payload:
                return None
            # can also access lists if needed, e.g., if key is '[1]'
            if (num_key := re.match(r"^\[(\d+)\]$", key)) is not None:
                try:
                    return payload[int(num_key.group(1))]
                except IndexError:
                    return None
            else:
                return payload.get(key, None)

        found = reduce(get_despite_none, jwt_path.split("."), jwt_payload)

        if found is None:  # compare to None, as it could also be empty
            raise KeyError(
                f"dot notation {jwt_path} not found in JWT request payload: {jwt_payload}"
            )
        return str(found)

    def _jwt_prepared_request(self) -> requests.PreparedRequest:
        """
        Return a prepared JWT requests.

        Note
        ----
        Override this method if your JWT procedure is not a POST request with basic auth
        """
        return requests.Request(
            "POST", self.jwt_url, auth=(self.user, self.passw)
        ).prepare()

    def _get_jwt_token(self) -> Optional[str]:
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
        response = requests.Session().send(self._jwt_prepared_request())
        response.raise_for_status()

        # catch json decoder errors if API has not implemented HTTP errors fully
        try:
            jwt_payload = response.json()
        except json.decoder.JSONDecodeError as e:
            logging.error(
                "Unable to unpack HTTP body response to get JWT token: " + str(e)
            )
            raise requests.HTTPError from e

        self.jwt_token = self._find_jwt_token(
            jwt_path=self.jwt_token_path, jwt_payload=jwt_payload
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
