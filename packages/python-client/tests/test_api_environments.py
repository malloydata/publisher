import respx
import httpx
import pytest

from malloy_publisher_sdk import Client
from malloy_publisher_sdk.api.environments import list_environments
from malloy_publisher_sdk.models import Environment


@pytest.mark.asyncio
async def test_list_environments_sync_and_async():
    """Test list_environments sync and async helpers using a mocked backend."""
    base_url = "http://test.local/api/v0"
    client = Client(base_url=base_url)

    fake_environments_response = [
        {
            "resource": "/environments/demo",
            "name": "demo",
            "readme": "Demo environment",
        },
        {
            "resource": "/environments/another",
            "name": "another",
            "readme": None,
        },
    ]

    # respx will intercept the outgoing request made by httpx inside the generated client
    route_path = "/environments"
    with respx.mock(base_url=base_url) as respx_mock:
        respx_mock.get(route_path).mock(
            return_value=httpx.Response(200, json=fake_environments_response)
        )

        # ---- sync variant ----
        environments_sync = list_environments.sync(client=client)
        assert isinstance(environments_sync, list)
        assert len(environments_sync) == 2
        assert all(isinstance(e, Environment) for e in environments_sync)
        assert environments_sync[0].name == "demo"

        # ---- async variant ----
        environments_async = await list_environments.asyncio(client=client)
        assert isinstance(environments_async, list)
        assert environments_async[1].name == "another"
