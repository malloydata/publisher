import respx
import httpx
import pytest

from malloy_publisher_sdk import Client
from malloy_publisher_sdk.api.projects import list_projects
from malloy_publisher_sdk.models import Project


@pytest.mark.asyncio
async def test_list_projects_sync_and_async():
    """Test list_projects sync and async helpers using a mocked backend."""
    base_url = "http://test.local/api/v0"
    client = Client(base_url=base_url)

    fake_projects_response = [
        {
            "resource": "/projects/demo",
            "name": "demo",
            "readme": "Demo project",
        },
        {
            "resource": "/projects/another",
            "name": "another",
            "readme": None,
        },
    ]

    # respx will intercept the outgoing request made by httpx inside the generated client
    route_path = "/projects"
    with respx.mock(base_url=base_url) as respx_mock:
        respx_mock.get(route_path).mock(
            return_value=httpx.Response(200, json=fake_projects_response)
        )

        # ---- sync variant ----
        projects_sync = list_projects.sync(client=client)
        assert isinstance(projects_sync, list)
        assert len(projects_sync) == 2
        assert all(isinstance(p, Project) for p in projects_sync)
        assert projects_sync[0].name == "demo"

        # ---- async variant ----
        projects_async = await list_projects.asyncio(client=client)
        assert isinstance(projects_async, list)
        assert projects_async[1].name == "another"
