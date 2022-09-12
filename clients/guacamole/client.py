import base64
import hashlib
import hmac
import os

import requests
from dotenv import load_dotenv

from .models import (
    GuacamoleAuthGenerateTokenResponse,
    GuacamoleConnectionsAndGroupsResponse,
    GuacamoleConnectionsListResponse,
)

load_dotenv()


class GuacamoleConnectionNotFound(Exception):
    pass


class GuacamoleHttpError(Exception):
    pass


class GuacamoleClient:
    """Provides functions to interact with Guacamole REST API.
    Guacamole REST API doc is available at :
    https://github.com/ridvanaltun/guacamole-rest-api-documentation
    """

    def __init__(self):
        self._guamacole_root_url = os.environ["GUACAMOLE_ROOT_URL"]

    def _get_token(self, username: str, password: str) -> str:
        response = requests.post(
            f"{self._guamacole_root_url}/api/tokens",
            data={"username": username, "password": password},
            timeout=5,
        )
        parsed_response = GuacamoleAuthGenerateTokenResponse(**response.json())
        return parsed_response.auth_token

    def _get_admin_token(self):
        return self._get_token(
            os.environ["GUACAMOLE_ADMIN_USERNAME"],
            os.environ["GUACAMOLE_ADMIN_PASSWORD"],
        )

    def get_connection_by_name(self, name: str) -> str:
        token = self._get_admin_token()
        response = requests.get(
            f"{self._guamacole_root_url}/api/session/data/mysql/connections?token={token}",
            timeout=5,
        )
        parsed_response = GuacamoleConnectionsListResponse(**response.json())

        for conn_id in parsed_response:
            conn = parsed_response[conn_id]
            if conn.name == name:
                return conn.identifier

        raise GuacamoleConnectionNotFound(f"Connection {name} not found")

    def create_connection(
        self,
        name: str,
        ip_address: str,
        username: str,
        password: str,
        port: str = "3389",
    ):
        """Creates a connection and returns its ID."""
        token = self._get_admin_token()
        requests.post(
            f"{self._guamacole_root_url}/api/session/data/mysql/connections?token={token}",
            json={
                "parentIdentifier": "ROOT",
                "name": name,
                "protocol": "rdp",
                "parameters": {
                    "port": port,
                    "hostname": ip_address,
                    "username": username,
                    "password": password,
                    "drive-name": "Project data",
                    "drive-path": f"/filetransfer/projects/{name}",
                    "security": "nla",
                    "ignore-cert": "true",
                    "resize-method": "display-update",
                    "enable-font-smoothing": "true",
                    "enable-drive": "true",
                    "create-drive-path": "true",
                    "color-depth": "24",
                    "read-only": "",
                    "swap-red-blue": "",
                    "cursor": "",
                    "clipboard-encoding": "",
                    "disable-copy": "",
                    "disable-paste": "",
                    "dest-port": "",
                    "recording-exclude-output": "",
                    "recording-exclude-mouse": "",
                    "recording-include-keys": "",
                    "create-recording-path": "",
                    "enable-sftp": "",
                    "sftp-port": "",
                    "sftp-server-alive-interval": "",
                    "enable-audio": "",
                    "disable-auth": "",
                    "gateway-port": "",
                    "server-layout": "",
                    "timezone": "",
                    "console": "",
                    "width": "",
                    "height": "",
                    "dpi": "",
                    "console-audio": "",
                    "disable-audio": "",
                    "enable-audio-input": "",
                    "enable-printing": "",
                    "enable-wallpaper": "",
                    "enable-theming": "",
                    "enable-full-window-drag": "",
                    "enable-desktop-composition": "",
                    "enable-menu-animations": "",
                    "disable-bitmap-caching": "",
                    "disable-offscreen-caching": "",
                    "disable-glyph-caching": "",
                    "preconnection-id": "",
                    "domain": "",
                    "gateway-hostname": "",
                    "gateway-username": "",
                    "gateway-password": "",
                    "gateway-domain": "",
                    "initial-program": "",
                    "client-name": "",
                    "printer-name": "",
                    "static-channels": "",
                    "remote-app": "",
                    "remote-app-dir": "",
                    "remote-app-args": "",
                    "preconnection-blob": "",
                    "load-balance-info": "",
                    "recording-path": "",
                    "recording-name": "",
                    "sftp-hostname": "",
                    "sftp-host-key": "",
                    "sftp-username": "",
                    "sftp-password": "",
                    "sftp-private-key": "",
                    "sftp-passphrase": "",
                    "sftp-root-directory": "",
                    "sftp-directory": "",
                },
                "attributes": {
                    "max-connections": "",
                    "max-connections-per-user": "",
                    "weight": "",
                    "failover-only": "",
                    "guacd-port": "",
                    "guacd-encryption": "",
                    "guacd-hostname": "",
                },
            },
            timeout=5,
        )

    def delete_connection(self, name: str):
        token = self._get_admin_token()
        connection_id = self.get_connection_by_name(name)
        # pylint: disable=consider-using-f-string
        response = requests.delete(
            "{}/api/session/data/mysql/connections/{}?token={}".format(
                self._guamacole_root_url, connection_id, token
            ),
            timeout=5,
        )
        if response.ok:
            return None
        raise GuacamoleHttpError(f"{response.text} [{response.status_code}]")

    def assign_user_to_connection(self, connection_id: str, username: str):
        token = self._get_admin_token()
        # pylint: disable=line-too-long
        return requests.patch(
            f"{self._guamacole_root_url}/api/session/data/mysql/users/{username}/permissions?token={token}",
            json=[
                {
                    "op": "add",
                    "path": f"/connectionPermissions/{connection_id}",
                    "value": "READ",
                }
            ],
            timeout=5,
        )

    def create_user_if_absent(self, username: str):
        token = self._get_admin_token()

        user_detail_resp = requests.get(
            f"{self._guamacole_root_url}/api/session/data/mysql/users/{username}?token={token}",
            timeout=5,
        )
        if not user_detail_resp.ok:
            password = get_password_for_username(
                username, os.environ["GUACAMOLE_SECRET_KEY"]
            )
            requests.post(
                f"{self._guamacole_root_url}/api/session/data/mysql/users?token={token}",
                json={
                    "username": username,
                    "password": password,
                    "attributes": {
                        "disabled": "",
                        "expired": "",
                        "access-window-start": "",
                        "access-window-end": "",
                        "valid-from": "",
                        "valid-until": "",
                        "timezone": None,
                        "guac-full-name": "",
                        "guac-organization": "",
                        "guac-organizational-role": "",
                    },
                },
                timeout=5,
            )

    def generate_connection_link(self, connection_id: str, user_id: str):
        token = self._get_token(
            username=user_id,
            password=get_password_for_username(
                username=user_id, key=os.environ["GUACAMOLE_SECRET_KEY"]
            ),
        )
        bytes_to_encode = bytes("\0".join([connection_id, "c", "mysql"]), "utf-8")
        client_identifier = base64.b64encode(bytes_to_encode).decode("utf-8")
        return f"{os.environ['GUACAMOLE_ROOT_URL']}/#/client/{client_identifier}?token={token}"

    def get_connections_and_groups(self) -> GuacamoleConnectionsAndGroupsResponse :
        token = self._get_admin_token()

        resp = requests.get(
            f"{self._guamacole_root_url}/api/session/data/mysql/connectionGroups/ROOT/tree?token={token}",  # pylint: disable=line-too-long
            timeout=5,
        )

        if not resp.ok:
            raise Exception(
                f"Error getting response ({resp.status_code}): {resp.json()['message']}"
            )

        data = GuacamoleConnectionsAndGroupsResponse(**resp.json())
        return data


def get_password_for_username(username: str, key: str) -> str:
    """Encrypt username with a key to use as a password."""
    return hmac.new(
        bytes(key, "utf-8"), bytes(username, "utf-8"), hashlib.sha256
    ).hexdigest()
