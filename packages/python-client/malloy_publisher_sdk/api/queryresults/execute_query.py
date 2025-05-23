from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.query_result import QueryResult
from ...types import UNSET, Response, Unset


def _get_kwargs(
    project_name: str,
    package_name: str,
    path: str,
    *,
    query: Union[Unset, str] = UNSET,
    source_name: Union[Unset, str] = UNSET,
    query_name: Union[Unset, str] = UNSET,
    version_id: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["query"] = query

    params["sourceName"] = source_name

    params["queryName"] = query_name

    params["versionId"] = version_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_name}/packages/{package_name}/queryResults/{path}".format(
            project_name=project_name,
            package_name=package_name,
            path=path,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, QueryResult]]:
    if response.status_code == 200:
        response_200 = QueryResult.from_dict(response.json())

        return response_200
    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400
    if response.status_code == 401:
        response_401 = Error.from_dict(response.json())

        return response_401
    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404
    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500
    if response.status_code == 501:
        response_501 = Error.from_dict(response.json())

        return response_501
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, QueryResult]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_name: str,
    package_name: str,
    path: str,
    *,
    client: Union[AuthenticatedClient, Client],
    query: Union[Unset, str] = UNSET,
    source_name: Union[Unset, str] = UNSET,
    query_name: Union[Unset, str] = UNSET,
    version_id: Union[Unset, str] = UNSET,
) -> Response[Union[Error, QueryResult]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        package_name (str):
        path (str):
        query (Union[Unset, str]):
        source_name (Union[Unset, str]):
        query_name (Union[Unset, str]):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryResult]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        package_name=package_name,
        path=path,
        query=query,
        source_name=source_name,
        query_name=query_name,
        version_id=version_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_name: str,
    package_name: str,
    path: str,
    *,
    client: Union[AuthenticatedClient, Client],
    query: Union[Unset, str] = UNSET,
    source_name: Union[Unset, str] = UNSET,
    query_name: Union[Unset, str] = UNSET,
    version_id: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, QueryResult]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        package_name (str):
        path (str):
        query (Union[Unset, str]):
        source_name (Union[Unset, str]):
        query_name (Union[Unset, str]):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryResult]
    """

    return sync_detailed(
        project_name=project_name,
        package_name=package_name,
        path=path,
        client=client,
        query=query,
        source_name=source_name,
        query_name=query_name,
        version_id=version_id,
    ).parsed


async def asyncio_detailed(
    project_name: str,
    package_name: str,
    path: str,
    *,
    client: Union[AuthenticatedClient, Client],
    query: Union[Unset, str] = UNSET,
    source_name: Union[Unset, str] = UNSET,
    query_name: Union[Unset, str] = UNSET,
    version_id: Union[Unset, str] = UNSET,
) -> Response[Union[Error, QueryResult]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        package_name (str):
        path (str):
        query (Union[Unset, str]):
        source_name (Union[Unset, str]):
        query_name (Union[Unset, str]):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryResult]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        package_name=package_name,
        path=path,
        query=query,
        source_name=source_name,
        query_name=query_name,
        version_id=version_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_name: str,
    package_name: str,
    path: str,
    *,
    client: Union[AuthenticatedClient, Client],
    query: Union[Unset, str] = UNSET,
    source_name: Union[Unset, str] = UNSET,
    query_name: Union[Unset, str] = UNSET,
    version_id: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, QueryResult]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        package_name (str):
        path (str):
        query (Union[Unset, str]):
        source_name (Union[Unset, str]):
        query_name (Union[Unset, str]):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryResult]
    """

    return (
        await asyncio_detailed(
            project_name=project_name,
            package_name=package_name,
            path=path,
            client=client,
            query=query,
            source_name=source_name,
            query_name=query_name,
            version_id=version_id,
        )
    ).parsed
