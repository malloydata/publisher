import pytest
from malloy_publisher_sdk import Client


def test_client_initialization():
    """Test that the API client can be initialized."""
    client = Client(base_url="http://localhost:4000/api/v0")
    assert client is not None
    # Accessing _base_url is checking an internal, but for basic init it's acceptable
    assert client._base_url == "http://localhost:4000/api/v0"


@pytest.mark.asyncio
async def test_client_can_prepare_async_call():
    """Test that the client can prepare and potentially make an async call.
    This doesn't actually make a call but tests the async setup part.
    """
    client = Client(base_url="http://localhost:4000/api/v0")
    assert client is not None

    # Example: Get the first API operation group (e.g., projects)
    # and its first async method. This is highly dependent on generated client structure.
    # We are looking for a method like `client.projects.list_projects.async_()`
    # For now, let's just assert the async httpx client can be retrieved.
    async_httpx_client = client.get_async_httpx_client()
    assert async_httpx_client is not None
    # httpx.URL might add a trailing slash if it's not present in the input string
    # So we compare the string representation and ensure we account for that.
    expected_base_url = "http://localhost:4000/api/v0"
    if not expected_base_url.endswith("/"):
        expected_base_url += "/"
    assert str(async_httpx_client.base_url) == expected_base_url

    # A more robust test would mock an actual async API call if the structure is known
    # e.g. if there's a top-level `list_projects_async` or similar.
    # from malloy_publisher_sdk.api.projects import list_projects
    # try:
    #     # This part would require mocking with respx if we want to unit test it
    #     # without hitting a live server.
    #     # For now, we are just checking if the client can be set up.
    #     pass # await list_projects.async_(client=client)
    # except Exception as e:
    #     pytest.fail(f"Failed to prepare async call: {e}")
