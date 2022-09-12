GUACAMOLE_CONNECTION_LIST_RESPONSE = {
    "1": {
        "name": "test",
        "identifier": "1",
        "parentIdentifier": "ROOT",
        "protocol": "rdp",
        "attributes": {
            "guacd-encryption": None,
            "failover-only": None,
            "weight": None,
            "max-connections": None,
            "guacd-hostname": "test",
            "guacd-port": "12343",
            "max-connections-per-user": None,
        },
        "activeConnections": 0,
    },
    "2": {
        "name": "test-02",
        "identifier": "2",
        "parentIdentifier": "ROOT",
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
        "lastActive": 1653897108000,
    },
}

GUACAMOLE_CONNECTIONS_AND_GROUPS_RESPONSE = {
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
