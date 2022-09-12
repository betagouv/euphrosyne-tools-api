from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# Authentication
class GuacamoleAuthGenerateTokenResponse(BaseModel):
    auth_token: str = Field("", alias="authToken")
    username: str
    data_source: str = Field("", alias="dataSource")
    available_data_sources: list[str] = Field([], alias="availableDataSources")


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


class GuacamoleConnectionsListData(BaseModel):
    name: str
    identifier: str
    parent_identifier: str = Field("", alias="parentIdentifier")
    protocol: str
    active_connections: int = Field(0, alias="activeConnections")
    last_active: Optional[datetime] = Field(None, alias="lastActive")
    attributes: GuacamoleConnectionsListDataAttributes


class GuacamoleConnectionsListResponse(BaseModel):
    __root__: dict[str, GuacamoleConnectionsListData]

    def __iter__(self):
        return iter(self.__root__)

    def __getitem__(self, item):
        return self.__root__[item]


class GuacamoleConnectionGroupAttribute(BaseModel):
    max_connections: Optional[str] = Field(None, alias="max-connections")
    max_connections_per_user: Optional[str] = Field(
        None, alias="max-connections-per-user"
    )
    enable_session_affinity: str = Field("", alias="enable-session-affinity")


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


class GuacamoleConnectionsAndGroupsResponse(BaseModel):
    name: str
    identifier: str
    type: str
    active_connections: int = Field(0, alias="activeConnections")
    child_connection_groups: list[GuacamoleConnectionGroupData] = Field(
        [], alias="childConnectionGroups"
    )
