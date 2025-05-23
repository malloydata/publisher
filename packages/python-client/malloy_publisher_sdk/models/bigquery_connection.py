from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BigqueryConnection")


@_attrs_define
class BigqueryConnection:
    """
    Attributes:
        default_project_id (Union[Unset, str]):
        billing_project_id (Union[Unset, str]):
        location (Union[Unset, str]):
        service_account_key_json (Union[Unset, str]):
        maximum_bytes_billed (Union[Unset, str]):
        query_timeout_milliseconds (Union[Unset, str]):
    """

    default_project_id: Union[Unset, str] = UNSET
    billing_project_id: Union[Unset, str] = UNSET
    location: Union[Unset, str] = UNSET
    service_account_key_json: Union[Unset, str] = UNSET
    maximum_bytes_billed: Union[Unset, str] = UNSET
    query_timeout_milliseconds: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        default_project_id = self.default_project_id

        billing_project_id = self.billing_project_id

        location = self.location

        service_account_key_json = self.service_account_key_json

        maximum_bytes_billed = self.maximum_bytes_billed

        query_timeout_milliseconds = self.query_timeout_milliseconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if default_project_id is not UNSET:
            field_dict["defaultProjectId"] = default_project_id
        if billing_project_id is not UNSET:
            field_dict["billingProjectId"] = billing_project_id
        if location is not UNSET:
            field_dict["location"] = location
        if service_account_key_json is not UNSET:
            field_dict["serviceAccountKeyJson"] = service_account_key_json
        if maximum_bytes_billed is not UNSET:
            field_dict["maximumBytesBilled"] = maximum_bytes_billed
        if query_timeout_milliseconds is not UNSET:
            field_dict["queryTimeoutMilliseconds"] = query_timeout_milliseconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        default_project_id = d.pop("defaultProjectId", UNSET)

        billing_project_id = d.pop("billingProjectId", UNSET)

        location = d.pop("location", UNSET)

        service_account_key_json = d.pop("serviceAccountKeyJson", UNSET)

        maximum_bytes_billed = d.pop("maximumBytesBilled", UNSET)

        query_timeout_milliseconds = d.pop("queryTimeoutMilliseconds", UNSET)

        bigquery_connection = cls(
            default_project_id=default_project_id,
            billing_project_id=billing_project_id,
            location=location,
            service_account_key_json=service_account_key_json,
            maximum_bytes_billed=maximum_bytes_billed,
            query_timeout_milliseconds=query_timeout_milliseconds,
        )

        bigquery_connection.additional_properties = d
        return bigquery_connection

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
