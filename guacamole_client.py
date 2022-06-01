import base64
import hashlib
import hmac
import os

import requests
from dotenv import load_dotenv

load_dotenv()


class GuacamoleConnectionNotFound(Exception):
    pass


class GuacamoleClient:
    """Provides an API to interact with Guacamole."""

    def __init__(self):
        self._guamacole_root_url = os.environ["GUACAMOLE_ROOT_URL"]

    def _get_token(self, username: str, password: str) -> str:
        response = requests.post(
            f"{self._guamacole_root_url}/api/tokens",
            data={"username": username, "password": password},
        )
        return response.json()["authToken"]

    def _get_admin_token(self):
        return self._get_token(
            os.environ["GUACAMOLE_ADMIN_USERNAME"],
            os.environ["GUACAMOLE_ADMIN_PASSWORD"],
        )

    def get_connection_by_name(self, name: str) -> str:
        token = self._get_admin_token()
        response = requests.get(
            f"{self._guamacole_root_url}/api/session/data/mysql/connections?token={token}"
        )
        try:
            return next(
                conn["identifier"]
                for conn in response.json().values()
                if conn["name"] == name
            )
        except StopIteration as error:
            raise GuacamoleConnectionNotFound() from error

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
                    "read-only": "",
                    "swap-red-blue": "",
                    "cursor": "",
                    "color-depth": "",
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
                    "security": "",
                    "disable-auth": "",
                    "ignore-cert": "",
                    "gateway-port": "",
                    "server-layout": "",
                    "timezone": "",
                    "console": "",
                    "width": "",
                    "height": "",
                    "dpi": "",
                    "resize-method": "",
                    "console-audio": "",
                    "disable-audio": "",
                    "enable-audio-input": "",
                    "enable-printing": "",
                    "enable-drive": "",
                    "create-drive-path": "",
                    "enable-wallpaper": "",
                    "enable-theming": "",
                    "enable-font-smoothing": "",
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
        )

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
        )

    def create_user_if_absent(self, username: str):
        token = self._get_admin_token()

        user_detail_resp = requests.get(
            f"{self._guamacole_root_url}/api/session/data/mysql/users/{username}?token={token}"
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


def get_password_for_username(username: str, key: str) -> str:
    """Encrypt username with a key to use as a password."""
    return hmac.new(
        bytes(key, "utf-8"), bytes(username, "utf-8"), hashlib.sha256
    ).hexdigest()
