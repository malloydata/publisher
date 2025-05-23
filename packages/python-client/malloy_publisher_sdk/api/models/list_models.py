from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.model import Model
from ...types import UNSET, Response, Unset


def _get_kwargs(
    project_name: str,
    package_name: str,
    *,
    version_id: Union[Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["versionId"] = version_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/projects/{project_name}/packages/{package_name}/models".format(
            project_name=project_name,
            package_name=package_name,
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, list["Model"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = Model.from_dict(response_200_item_data)

            response_200.append(response_200_item)

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
    if response.status_code == 501:
        response_501 = Error.from_dict(response.json())

        return response_501
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, list["Model"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    project_name: str,
    package_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    version_id: Union[Unset, str] = UNSET,
) -> Response[Union[Error, list["Model"]]]:
    """Returns a list of relative paths to the models in the package.

    Args:
        project_name (str):
        package_name (str):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, list['Model']]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        package_name=package_name,
        version_id=version_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    project_name: str,
    package_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    version_id: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, list["Model"]]]:
    """Returns a list of relative paths to the models in the package.

    Args:
        project_name (str):
        package_name (str):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, list['Model']]
    """

    return sync_detailed(
        project_name=project_name,
        package_name=package_name,
        client=client,
        version_id=version_id,
    ).parsed


async def asyncio_detailed(
    project_name: str,
    package_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    version_id: Union[Unset, str] = UNSET,
) -> Response[Union[Error, list["Model"]]]:
    """Returns a list of relative paths to the models in the package.

    Args:
        project_name (str):
        package_name (str):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, list['Model']]]
    """

    kwargs = _get_kwargs(
        project_name=project_name,
        package_name=package_name,
        version_id=version_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    project_name: str,
    package_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    version_id: Union[Unset, str] = UNSET,
) -> Optional[Union[Error, list["Model"]]]:
    """Returns a list of relative paths to the models in the package.

    Args:
        project_name (str):
        package_name (str):
        version_id (Union[Unset, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, list['Model']]
    """

    return (
        await asyncio_detailed(
            project_name=project_name,
            package_name=package_name,
            client=client,
            version_id=version_id,
        )
    ).parsed
