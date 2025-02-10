from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slugify import slugify

from auth import Project, User, get_current_user
from clients.azure import DataAzureClient, VMAzureClient
from clients.guacamole import GuacamoleClient
from dependencies import (get_guacamole_client, get_storage_azure_client,
                          get_vm_azure_client)
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
    app.dependency_overrides[get_storage_azure_client] = lambda: MagicMock(
        spec=DataAzureClient
    )
    app.dependency_overrides[get_guacamole_client] = lambda: MagicMock(
        spec=GuacamoleClient
    )
    app.dependency_overrides[get_current_user] = get_current_user_override
    return _client


@pytest.fixture(autouse=True)
def setenv(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("JWT_SECRET_KEY", "secret")


async def get_current_user_override():
    projet_name = "project 01"
    return User(
        id="1",
        projects=[Project(id=1, name=projet_name, slug=slugify(projet_name))],
        is_admin=False,
    )
