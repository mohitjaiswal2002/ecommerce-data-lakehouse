import json
import os

import pytest
import requests

from utils import api_utils


class FakeResponse:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload or {}
        self.status_code = status_code
        self.url = "https://example.test"

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def __bool__(self):
        # Mirrors requests.Response, which is falsy for error status codes.
        return self.status_code < 400


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(api_utils.time, "sleep", lambda _: None)


def test_fetch_all_pages_collects_every_page(monkeypatch):
    records = [{"id": i} for i in range(1, 251)]
    calls = []

    def fake_get(url, params, timeout):
        calls.append(params)
        page = records[params["skip"]: params["skip"] + params["limit"]]
        return FakeResponse({"products": page, "total": len(records)})

    monkeypatch.setattr(api_utils.requests, "get", fake_get)

    result = api_utils.fetch_all_pages("https://example.test/products", limit=100)

    assert result == records
    assert [c["skip"] for c in calls] == [0, 100, 200]


def test_fetch_with_retry_recovers_from_server_error(monkeypatch):
    responses = iter([FakeResponse(status_code=503), FakeResponse({"ok": True})])
    monkeypatch.setattr(api_utils.requests, "get", lambda *a, **k: next(responses))

    assert api_utils.fetch_with_retry("https://example.test") == {"ok": True}


def test_fetch_with_retry_does_not_retry_client_error(monkeypatch):
    calls = []

    def fake_get(*args, **kwargs):
        calls.append(1)
        return FakeResponse(status_code=404)

    monkeypatch.setattr(api_utils.requests, "get", fake_get)

    with pytest.raises(requests.exceptions.HTTPError):
        api_utils.fetch_with_retry("https://example.test", retries=3)
    assert len(calls) == 1


def test_fetch_with_retry_gives_up_after_all_attempts(monkeypatch):
    def fake_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError("down")

    monkeypatch.setattr(api_utils.requests, "get", fake_get)

    with pytest.raises(RuntimeError, match="All 3 attempts failed"):
        api_utils.fetch_with_retry("https://example.test", retries=2)


def test_save_bronze_snapshot_keeps_api_envelope(tmp_path):
    records = [{"id": 1}, {"id": 2}]

    output_file = api_utils.save_bronze_snapshot(records, "users", str(tmp_path))

    assert os.path.basename(output_file).startswith("users_")
    with open(output_file, encoding="utf-8") as file:
        assert json.load(file) == {"users": records, "total": 2}
