from typing import Any

from clients.guacamole import models


def test_parse_auth_response():
    json = {
        "authToken": "token",
        "username": "guac_username",
        "dataSource": "mysql",
        "availableDataSources": ["mysql", "mysql-shared"],
    }

    parsed_data = models.GuacamoleAuthGenerateTokenResponse.parse_obj(json)
    assert parsed_data.auth_token == json["authToken"]
    assert parsed_data.username == json["username"]
    assert parsed_data.data_source == json["dataSource"]
    assert parsed_data.available_data_sources[0] == json["availableDataSources"][0]


def test_parse_list_connections_response():
    json = {
        "39": {
            "name": "update-setup-clone",
            "identifier": "39",
            "parentIdentifier": "2",
            "protocol": "rdp",
            "attributes": {
                "guacd-encryption": "ssl",
                "failover-only": "true",
                "weight": "12",
                "max-connections": "12",
                "guacd-hostname": "guadc",
                "guacd-port": "12",
                "max-connections-per-user": "5",
            },
            "activeConnections": 0,
        },
        "30": {
            "name": "update-setup",
            "identifier": "30",
            "parentIdentifier": "2",
            "protocol": "rdp",
            "attributes": {
                "guacd-encryption": None,
                "failover-only": None,
                "weight": None,
                "max-connections": None,
                "guacd-hostname": None,
                "guacd-port": None,
                "max-connections-per-user": None,
            },
            "activeConnections": 0,
            "lastActive": 1662369688000,
        },
    }

    parsed_data = models.GuacamoleConnectionsListResponse.parse_obj(json)
    assert isinstance(parsed_data["30"], models.GuacamoleConnectionsListData)
    assert isinstance(parsed_data["39"], models.GuacamoleConnectionsListData)

    # Assert 30
    update_setup_json = json["30"]
    update_setup_data = parsed_data["30"]
    assert_connections(update_setup_data, update_setup_json)

    # Assert 39
    update_setup_clone_json = json["39"]
    update_setup_clone_data = parsed_data["39"]
    assert_connections(update_setup_clone_data, update_setup_clone_json)


def test_parse_list_connections_and_groups():
    json = {
        "name": "ROOT",
        "identifier": "ROOT",
        "type": "ORGANIZATIONAL",
        "activeConnections": 0,
        "childConnectionGroups": [
            {
                "name": "default",
                "identifier": "1",
                "parentIdentifier": "ROOT",
                "type": "ORGANIZATIONAL",
                "activeConnections": 0,
                "attributes": {
                    "max-connections": None,
                    "max-connections-per-user": None,
                    "enable-session-affinity": "",
                },
            },
            {
                "name": "imagery",
                "identifier": "2",
                "parentIdentifier": "ROOT",
                "type": "ORGANIZATIONAL",
                "activeConnections": 0,
                "childConnections": [
                    {
                        "name": "update-setup",
                        "identifier": "30",
                        "parentIdentifier": "2",
                        "protocol": "rdp",
                        "attributes": {
                            "guacd-encryption": None,
                            "failover-only": None,
                            "weight": None,
                            "max-connections": None,
                            "guacd-hostname": None,
                            "guacd-port": None,
                            "max-connections-per-user": None,
                        },
                        "sharingProfiles": [
                            {
                                "name": "share-setup",
                                "identifier": "1",
                                "primaryConnectionIdentifier": "30",
                                "attributes": {},
                            }
                        ],
                        "activeConnections": 0,
                        "lastActive": 1662369688000,
                    },
                    {
                        "name": "update-setup-clone",
                        "identifier": "39",
                        "parentIdentifier": "2",
                        "protocol": "rdp",
                        "attributes": {
                            "guacd-encryption": "ssl",
                            "failover-only": "true",
                            "weight": "12",
                            "max-connections": "12",
                            "guacd-hostname": "guadc",
                            "guacd-port": "12",
                            "max-connections-per-user": "5",
                        },
                        "activeConnections": 0,
                    },
                ],
                "attributes": {
                    "max-connections": None,
                    "max-connections-per-user": None,
                    "enable-session-affinity": "",
                },
            },
        ],
        "attributes": {},
    }

    parsed_data = models.GuacamoleConnectionsAndGroupsResponse.parse_obj(json)
    assert isinstance(parsed_data, models.GuacamoleConnectionsAndGroupsResponse)
    assert parsed_data.name == json["name"]
    assert parsed_data.identifier == json["identifier"]
    assert parsed_data.type == json["type"]
    assert parsed_data.active_connections == json["activeConnections"]
    assert isinstance(parsed_data.child_connection_groups, list)
    assert isinstance(
        parsed_data.child_connection_groups[0], models.GuacamoleConnectionGroupData
    )
    assert isinstance(
        parsed_data.child_connection_groups[1], models.GuacamoleConnectionGroupData
    )


def assert_connection_group(
    data: models.GuacamoleConnectionGroupData, json: dict[str, Any]
):
    assert data.name == json["name"]
    assert data.identifier == json["identifier"]
    assert data.parent_identifier == json["parentIdentifier"]
    assert data.type == json["type"]
    assert data.active_connections == json["activeConnections"]

    assert data.attributes.max_connections == json["attributes"]["max-connections"]
    assert (
        data.attributes.max_connections_per_user
        == json["attributes"]["max-connections-per-user"]
    )
    assert (
        data.attributes.enable_session_affinity
        == json["attributes"]["enable-session-affinity"]
    )

    if len(data.child_connections) > 0:
        for i, connection in enumerate(data.child_connections):
            connection_json = json["childConnections"][i]
            assert_connections(connection, connection_json)


def assert_connections(data: models.GuacamoleConnectionsListData, json: dict[str, Any]):
    assert data.name == json["name"]
    assert data.identifier == json["identifier"]
    assert data.parent_identifier == json["parentIdentifier"]
    assert data.protocol == json["protocol"]
    assert data.active_connections == json["activeConnections"]
    assert isinstance(data.attributes, models.GuacamoleConnectionsListDataAttributes)

    assert_connections_attributes(data.attributes, json["attributes"])


def assert_connections_attributes(
    data: models.GuacamoleConnectionsListDataAttributes, json: dict[str, Any]
):
    assert data.guacd_encryption == json["guacd-encryption"]
    assert data.failover_only == json["failover-only"]
    assert data.weight == json["weight"]
    assert data.max_connections == json["max-connections"]
    assert data.max_connections_per_user == json["max-connections-per-user"]
    assert data.guacd_hostname == json["guacd-hostname"]
    assert data.guacd_port == json["guacd-port"]
