from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient
from jwt import PyJWKClient
from jwt.algorithms import RSAAlgorithm
from slugify import slugify

import auth
from auth import Project, User, get_current_user
from clients.azure import DataAzureClient, VMAzureClient
from clients.guacamole import GuacamoleClient
from dependencies import (
    get_guacamole_client,
    get_project_data_client,
    get_vm_azure_client,
)
from main import app as _app

_client = TestClient(_app)


@pytest.fixture(name="app")
def fixture_app():
    return _app


@pytest.fixture(name="client")
def fixture_client(app: FastAPI):
    app.dependency_overrides[get_vm_azure_client] = lambda: MagicMock(
        spec=VMAzureClient
    )
    app.dependency_overrides[get_project_data_client] = lambda: MagicMock(
        spec=DataAzureClient
    )
    app.dependency_overrides[get_guacamole_client] = lambda: MagicMock(
        spec=GuacamoleClient
    )
    app.dependency_overrides[get_current_user] = get_current_user_override
    return _client


# Euphrosyne's token signing key and the key set it publishes.
EUPHROSYNE_URL = "http://euphrosyne.test"
EUPHROSYNE_PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
EUPHROSYNE_KID = "euphrosyne-key"
EUPHROSYNE_JWKS = {
    "keys": [
        {
            **RSAAlgorithm.to_jwk(EUPHROSYNE_PRIVATE_KEY.public_key(), as_dict=True),
            "kid": EUPHROSYNE_KID,
            "use": "sig",
            "alg": "RS256",
        }
    ]
}


class StaticJWKClient(PyJWKClient):
    """Serves EUPHROSYNE_JWKS instead of fetching /.well-known/jwks.json."""

    def fetch_data(self):
        return EUPHROSYNE_JWKS


@pytest.fixture(autouse=True)
def setenv(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", EUPHROSYNE_URL)
    monkeypatch.setenv("PATH_TOKEN_SECRET", "secret")
    monkeypatch.setenv("EUPHROSYNE_OIDC_CLIENT_ID", "euphrosyne-tools-api")
    monkeypatch.setenv("EUPHROSYNE_OIDC_CLIENT_SECRET", "client-secret")
    monkeypatch.setattr(
        auth,
        "_jwks_client",
        lambda: StaticJWKClient(f"{EUPHROSYNE_URL}/.well-known/jwks.json"),
    )


async def get_current_user_override():
    projet_name = "project 01"
    return User(
        id="1",
        projects=[Project(id=1, name=projet_name, slug=slugify(projet_name))],
        is_admin=False,
    )
