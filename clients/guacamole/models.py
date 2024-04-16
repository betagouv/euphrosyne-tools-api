from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, RootModel


# Authentication
class GuacamoleAuthGenerateTokenResponse(BaseModel):
    auth_token: str = Field("", alias="authToken")
    username: str
    data_source: str = Field("", alias="dataSource")
    available_data_sources: list[str] = Field([], alias="availableDataSources")

    model_config = ConfigDict(populate_by_name=True)


# Connections
class GuacamoleConnectionsListDataAttributes(BaseModel):
    guacd_encryption: Optional[str] = Field(None, alias="guacd-encryption")
    failover_only: Optional[str] = Field(None, alias="failover-only")
    weight: Optional[str] = None
    max_connections: Optional[str] = Field(None, alias="max-connections")
    max_connections_per_user: Optional[str] = Field(
        None, alias="max-connections-per-user"
    )
    guacd_hostname: Optional[str] = Field(None, alias="guacd-hostname")
    guacd_port: Optional[str] = Field(None, alias="guacd-port")

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionsListData(BaseModel):
    name: str
    identifier: str
    parent_identifier: str = Field("", alias="parentIdentifier")
    protocol: str
    active_connections: int = Field(0, alias="activeConnections")
    last_active: Optional[datetime] = Field(None, alias="lastActive")
    attributes: GuacamoleConnectionsListDataAttributes

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionsListResponse(RootModel):
    root: dict[str, GuacamoleConnectionsListData]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item):
        return self.root[item]

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionGroupAttribute(BaseModel):
    max_connections: Optional[str] = Field(None, alias="max-connections")
    max_connections_per_user: Optional[str] = Field(
        None, alias="max-connections-per-user"
    )
    enable_session_affinity: str = Field("", alias="enable-session-affinity")

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionGroupData(BaseModel):
    name: str
    identifier: str
    type: str
    parent_identifier: str = Field("", alias="parentIdentifier")
    active_connections: int = Field(0, alias="activeConnections")
    attributes: GuacamoleConnectionGroupAttribute
    child_connections: list[GuacamoleConnectionsListData] = Field(
        [], alias="childConnections"
    )

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionsAndGroupsResponse(BaseModel):
    name: str
    identifier: str
    type: str
    active_connections: int = Field(0, alias="activeConnections")
    child_connection_groups: list[GuacamoleConnectionGroupData] = Field(
        [], alias="childConnectionGroups"
    )

    model_config = ConfigDict(populate_by_name=True)


# Create connection input
class GuacamoleConnectionCreateParametersData(BaseModel):
    port: str = Field(...)
    hostname: str = Field(...)
    username: str = Field(...)
    password: str = Field(...)
    drive_name: str = Field("", alias="drive-name")
    drive_path: str = Field("", alias="drive-path")
    security: str = Field("nla")
    ignore_cert: str = Field("true", alias="ignore-cert")
    resize_method: str = Field("display-update", alias="resize-method")
    enable_font_smoothing: str = Field("true", alias="enable-font-smoothing")
    enable_drive: str = Field("true", alias="enable-drive")
    create_drive_path: str = Field("true", alias="create-drive-path")
    color_depth: str = Field("24", alias="color-depth")
    read_only: str = Field("", alias="read-only")
    swap_red_blue: str = Field("", alias="swap-red-blue")
    cursor: str = Field("")
    clipboard_encoding: str = Field("", alias="clipboard-encoding")
    disable_copy: str = Field("", alias="disable-copy")
    disable_paste: str = Field("", alias="disable-paste")
    dest_port: str = Field("", alias="dest-port")
    recording_exclude_output: str = Field("", alias="recording-exclude-output")
    recording_exclude_mouse: str = Field("", alias="recording-exclude-mouse")
    recording_include_keys: str = Field("", alias="recording-include-keys")
    create_recording_path: str = Field("", alias="create-recording-path")
    enable_audio: str = Field("", alias="enable-audio")
    disable_auth: str = Field("", alias="disable-auth")
    gateway_port: str = Field("", alias="gateway-port")
    server_layout: str = Field("", alias="server-layout")
    timezone: str = Field("")
    console: str = Field("")
    width: str = Field("")
    height: str = Field("")
    dpi: str = Field("")
    console_audio: str = Field("", alias="console-audio")
    disable_audio: str = Field("", alias="disable-audio")
    enable_audio_input: str = Field("", alias="enable-audio-input")
    enable_printing: str = Field("", alias="enable-printing")
    enable_wallpaper: str = Field("", alias="enable-wallpaper")
    enable_theming: str = Field("", alias="enable-theming")
    enable_full_window_drag: str = Field("", alias="enable-full-window-drag")
    enable_desktop_composition: str = Field("", alias="enable-desktop-composition")
    enable_menu_animation: str = Field("", alias="enable-menu-animation")
    disable_bitmap_caching: str = Field("", alias="disable-bitmap-caching")
    disable_offscreen_caching: str = Field("", alias="disable-offscreen-cachine")
    disable_glyph_caching: str = Field("", alias="disable-glyph-caching")
    preconnection_id: str = Field("", alias="preconnection-id")
    domain: str = Field("")
    gateway_hostname: str = Field("", alias="gateway-hostname")
    gateway_username: str = Field("", alias="gateway-username")
    gateway_domain: str = Field("", alias="gateway-domain")
    initial_program: str = Field("", alias="initial-program")
    client_name: str = Field("", alias="client-name")
    printer_name: str = Field("", alias="printer-name")
    static_channels: str = Field("", alias="static-channels")
    remote_app: str = Field("", alias="remote-app")
    remote_app_dir: str = Field("", alias="remote-app-dir")
    remote_app_args: str = Field("", alias="remote-app-args")
    preconnection_blob: str = Field("", alias="preconnection-blob")
    load_balance_info: str = Field("", alias="load-balance-info")
    recording_path: str = Field("", alias="recording-path")
    recording_name: str = Field("", alias="recoding-name")
    enable_sftp: str = Field("", alias="enable-sftp")
    sftp_hostname: str = Field("", alias="sftp-hostname")
    sftp_host_key: str = Field("", alias="sftp-host-key")
    sftp_username: str = Field("", alias="sftp-username")
    sftp_password: str = Field("", alias="sftp-password")
    sftp_private_key: str = Field("", alias="sftp-private-key")
    sftp_passphrase: str = Field("", alias="sftp-passphrase")
    sftp_root_directory: str = Field("", alias="sftp-root-directory")
    sftp_directory: str = Field("", alias="sftp-directory")
    sftp_port: str = Field("", alias="sftp-port")
    sftp_server_alive_interval: str = Field("", alias="sftp-server-alive-internal")

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleConnectionCreateInput(BaseModel):
    parent_identifier: str = Field(alias="parentIdentifier")
    name: str
    protocol: str
    attributes: GuacamoleConnectionsListDataAttributes = Field(
        GuacamoleConnectionsListDataAttributes()
    )
    parameters: GuacamoleConnectionCreateParametersData

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleUserPermissionInput(BaseModel):
    op: str = Field(...)
    path: str = Field(...)
    value: str = Field(...)

    model_config = ConfigDict(populate_by_name=True)


# User


class GuacamoleUserAttributes(BaseModel):
    disable: str = Field("")
    expired: str = Field("")
    access_window_start: str = Field("", alias="access-window-start")
    access_window_end: str = Field("", alias="access-window-end")
    valid_from: str = Field("", alias="valid-from")
    valid_until: str = Field("", alias="valid-until")
    timezone: Optional[str] = Field(None)
    guac_full_name: str = Field("", alias="guac-full-name")
    guac_organization: str = Field("", alias="guac-organization")
    guac_organization_role: str = Field("", alias="guac-organization-role")

    model_config = ConfigDict(populate_by_name=True)


class GuacamoleCreateUserInput(BaseModel):
    username: str = Field(...)
    password: str = Field(...)
    attributes: GuacamoleUserAttributes = Field(GuacamoleUserAttributes())

    model_config = ConfigDict(populate_by_name=True)
