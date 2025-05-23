from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    project_name: str,
    connection_name: str,
    schema_name: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_name}/connections/{connection_name}/schemas/{schema_name}/tables".format(
            project_name=project_name,
            connection_name=connection_name,
            schema_name=schema_name,
        ),
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, list[str]]]:
    if response.status_code == 200:
        response_200 = cast(list[str], response.json())

        return response_200
    if response.status_code == 401:
        response_401 = Error.from_dict(response.json())

        return response_401
    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404
    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, list[str]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_name: str,
    connection_name: str,
    schema_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[Error, list[str]]]:
    """Returns a list of tables available in the specified schema.

    Args:
        project_name (str):
        connection_name (str):
        schema_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, list[str]]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        connection_name=connection_name,
        schema_name=schema_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_name: str,
    connection_name: str,
    schema_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[Error, list[str]]]:
    """Returns a list of tables available in the specified schema.

    Args:
        project_name (str):
        connection_name (str):
        schema_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, list[str]]
    """

    return sync_detailed(
        project_name=project_name,
        connection_name=connection_name,
        schema_name=schema_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    project_name: str,
    connection_name: str,
    schema_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[Error, list[str]]]:
    """Returns a list of tables available in the specified schema.

    Args:
        project_name (str):
        connection_name (str):
        schema_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, list[str]]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        connection_name=connection_name,
        schema_name=schema_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_name: str,
    connection_name: str,
    schema_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[Error, list[str]]]:
    """Returns a list of tables available in the specified schema.

    Args:
        project_name (str):
        connection_name (str):
        schema_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, list[str]]
    """

    return (
        await asyncio_detailed(
            project_name=project_name,
            connection_name=connection_name,
            schema_name=schema_name,
            client=client,
        )
    ).parsed
