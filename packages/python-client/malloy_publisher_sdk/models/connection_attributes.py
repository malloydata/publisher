from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ConnectionAttributes")


@_attrs_define
class ConnectionAttributes:
    """
    Attributes:
        dialect_name (Union[Unset, str]):
        is_pool (Union[Unset, bool]):
        can_persist (Union[Unset, bool]):
        can_stream (Union[Unset, bool]):
    """

    dialect_name: Union[Unset, str] = UNSET
    is_pool: Union[Unset, bool] = UNSET
    can_persist: Union[Unset, bool] = UNSET
    can_stream: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        dialect_name = self.dialect_name

        is_pool = self.is_pool

        can_persist = self.can_persist

        can_stream = self.can_stream

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if dialect_name is not UNSET:
            field_dict["dialectName"] = dialect_name
        if is_pool is not UNSET:
            field_dict["isPool"] = is_pool
        if can_persist is not UNSET:
            field_dict["canPersist"] = can_persist
        if can_stream is not UNSET:
            field_dict["canStream"] = can_stream

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        dialect_name = d.pop("dialectName", UNSET)

        is_pool = d.pop("isPool", UNSET)

        can_persist = d.pop("canPersist", UNSET)

        can_stream = d.pop("canStream", UNSET)

        connection_attributes = cls(
            dialect_name=dialect_name,
            is_pool=is_pool,
            can_persist=can_persist,
            can_stream=can_stream,
        )

        connection_attributes.additional_properties = d
        return connection_attributes

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
