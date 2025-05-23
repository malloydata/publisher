from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.query_data import QueryData
from ...types import UNSET, Response, Unset


def _get_kwargs(
    project_name: str,
    connection_name: str,
    *,
    sql_statement: Union[Unset, str] = UNSET,
    options: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["sqlStatement"] = sql_statement

    params["options"] = options

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_name}/connections/{connection_name}/queryData".format(
            project_name=project_name,
            connection_name=connection_name,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, QueryData]]:
    if response.status_code == 200:
        response_200 = QueryData.from_dict(response.json())

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
) -> Response[Union[Error, QueryData]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_name: str,
    connection_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    sql_statement: Union[Unset, str] = UNSET,
    options: Union[Unset, str] = UNSET,
) -> Response[Union[Error, QueryData]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        connection_name (str):
        sql_statement (Union[Unset, str]):
        options (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryData]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        connection_name=connection_name,
        sql_statement=sql_statement,
        options=options,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_name: str,
    connection_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    sql_statement: Union[Unset, str] = UNSET,
    options: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, QueryData]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        connection_name (str):
        sql_statement (Union[Unset, str]):
        options (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryData]
    """

    return sync_detailed(
        project_name=project_name,
        connection_name=connection_name,
        client=client,
        sql_statement=sql_statement,
        options=options,
    ).parsed


async def asyncio_detailed(
    project_name: str,
    connection_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    sql_statement: Union[Unset, str] = UNSET,
    options: Union[Unset, str] = UNSET,
) -> Response[Union[Error, QueryData]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        connection_name (str):
        sql_statement (Union[Unset, str]):
        options (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, QueryData]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        connection_name=connection_name,
        sql_statement=sql_statement,
        options=options,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_name: str,
    connection_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    sql_statement: Union[Unset, str] = UNSET,
    options: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, QueryData]]:
    """Returns a query and its results.

    Args:
        project_name (str):
        connection_name (str):
        sql_statement (Union[Unset, str]):
        options (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, QueryData]
    """

    return (
        await asyncio_detailed(
            project_name=project_name,
            connection_name=connection_name,
            client=client,
            sql_statement=sql_statement,
            options=options,
        )
    ).parsed
