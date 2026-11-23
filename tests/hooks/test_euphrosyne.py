import unittest
from unittest import mock

import pytest

from clients.euphrosyne_oidc import EuphrosyneTokenError
from hooks.euphrosyne import post_data_access_event


class TestPostDataEvent(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._requests_patcher = mock.patch("hooks.euphrosyne.requests")
        self.requests_mock = self._requests_patcher.start()
        self.monkeypatch = pytest.MonkeyPatch()

    def tearDown(self) -> None:
        super().tearDown()
        self._requests_patcher.stop()

    def test_post_data_access_event_returns_none_when_url_not_set(self):
        assert post_data_access_event("path", "12") is None

    def test_post_data_access_event_success(self):
        self.monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
        with mock.patch(
            "hooks.euphrosyne.get_euphrosyne_backend_token",
            return_value="token",
        ):
            self.requests_mock.post.return_value.ok = True

            post_data_access_event("path", "12")
        self.requests_mock.post.assert_called_once_with(
            "http://localhost/api/data-request/access-event",
            headers={
                "Authorization": "Bearer token",
            },
            json={
                "path": "path",
                "data_request": 12,
            },
        )

    def test_post_data_access_event_log_when_failed(self):
        self.monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
        with mock.patch(
            "hooks.euphrosyne.get_euphrosyne_backend_token",
            return_value="token",
        ):
            self.requests_mock.post.return_value.ok = False
            with mock.patch("hooks.euphrosyne.logger") as logger_mock:
                post_data_access_event("path", "12")
                logger_mock.error.assert_called()

    def test_post_data_access_event_logs_when_no_token(self):
        self.monkeypatch.setenv("EUPHROSYNE_BACKEND_URL", "http://localhost")
        with mock.patch(
            "hooks.euphrosyne.get_euphrosyne_backend_token",
            side_effect=EuphrosyneTokenError("refused"),
        ), mock.patch("hooks.euphrosyne.logger") as logger_mock:
            post_data_access_event("path", "12")
        logger_mock.error.assert_called()
        self.requests_mock.post.assert_not_called()
