"""Tests for MCP transport selection."""

import pytest

from mcp_config import resolve_host, resolve_port, resolve_transport


class TestResolveTransport:
    """Tests for resolve_transport()."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "stdio"),
            ("", "stdio"),
            ("   ", "stdio"),
            ("\t\n", "stdio"),
            ("  STDIO ", "stdio"),
            ("http", "streamable-http"),
            ("Http", "streamable-http"),
            ("streamable-http", "streamable-http"),
            ("streamable_http", "streamable-http"),
            ("sse", "sse"),
            ("dual", "streamable-http"),
            ("all", "streamable-http"),
        ],
    )
    def test_valid_values(self, value, expected):
        assert resolve_transport(value) == expected

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="Invalid WHATSAPP_MCP_TRANSPORT"):
            resolve_transport("websocket")


class TestResolveHost:
    """Tests for resolve_host()."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, "127.0.0.1"),
            ("", "127.0.0.1"),
            ("   ", "127.0.0.1"),
            ("\t\n", "127.0.0.1"),
            (" 127.0.0.1 ", "127.0.0.1"),
            ("0.0.0.0", "0.0.0.0"),
        ],
    )
    def test_values(self, value, expected):
        assert resolve_host(value) == expected


class TestResolvePort:
    """Tests for resolve_port()."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, 8000),
            ("", 8000),
            ("   ", 8000),
            ("\t\n", 8000),
            ("9000", 9000),
            (" 9000 ", 9000),
            ("1", 1),
            ("65535", 65535),
        ],
    )
    def test_valid_values(self, value, expected):
        assert resolve_port(value) == expected

    def test_non_integer_raises(self):
        with pytest.raises(ValueError, match="Invalid WHATSAPP_MCP_PORT"):
            resolve_port("not-a-number")

    def test_out_of_range_raises(self):
        for value in ("0", "-1", "65536"):
            with pytest.raises(ValueError, match="Invalid WHATSAPP_MCP_PORT"):
                resolve_port(value)


class TestCreateASGIApp:
    """Tests for create_asgi_app() dual transport and authentication."""

    def test_health_check_endpoints(self):
        from starlette.testclient import TestClient
        from main import create_asgi_app

        app = create_asgi_app(mount_path="/mcp/whatsapp", auth_token="test-token")
        client = TestClient(app)

        # Root and health without event-stream should return 200 without auth
        res = client.get("/")
        assert res.status_code == 200
        assert res.json()["status"] == "healthy"

        res = client.get("/health")
        assert res.status_code == 200
        assert res.json()["status"] == "healthy"

        res = client.get("/mcp/whatsapp")
        assert res.status_code == 200
        assert res.json()["status"] == "healthy"

        res = client.get("/mcp/whatsapp/health")
        assert res.status_code == 200
        assert res.json()["status"] == "healthy"

    def test_auth_rejection(self):
        from starlette.testclient import TestClient
        from main import create_asgi_app

        app = create_asgi_app(mount_path="/mcp/whatsapp", auth_token="test-token")
        client = TestClient(app)

        # Unauthenticated SSE request should return 401
        res = client.get("/sse")
        assert res.status_code == 401
        assert "WWW-Authenticate" in res.headers

        res = client.get("/mcp/whatsapp/sse")
        assert res.status_code == 401

        # Unauthenticated Streamable HTTP POST should return 401
        res = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        assert res.status_code == 401

    def test_auth_acceptance_bearer(self):
        from starlette.testclient import TestClient
        from main import create_asgi_app

        app = create_asgi_app(mount_path="/mcp/whatsapp", auth_token="test-token")
        with TestClient(app) as client:
            headers = {"Authorization": "Bearer test-token", "Accept": "application/json, text/event-stream"}
            init_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"}
                }
            }
            res = client.post("/mcp", json=init_msg, headers=headers)
            assert res.status_code == 200

    def test_auth_acceptance_query_token(self):
        from starlette.testclient import TestClient
        from main import create_asgi_app

        app = create_asgi_app(mount_path="/mcp/whatsapp", auth_token="test-token")
        with TestClient(app) as client:
            headers = {"Accept": "application/json, text/event-stream"}
            init_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"}
                }
            }
            res = client.post("/mcp?token=test-token", json=init_msg, headers=headers)
            assert res.status_code == 200

