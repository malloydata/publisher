from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SchemaName")


@_attrs_define
class SchemaName:
    """A schema name in a Connection.

    Attributes:
        name (Union[Unset, str]): Name of the schema
        is_default (Union[Unset, bool]): Whether this schema is the default schema
        is_hidden (Union[Unset, bool]): Whether this schema is hidden
    """

    name: Union[Unset, str] = UNSET
    is_default: Union[Unset, bool] = UNSET
    is_hidden: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        is_default = self.is_default

        is_hidden = self.is_hidden

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if is_default is not UNSET:
            field_dict["isDefault"] = is_default
        if is_hidden is not UNSET:
            field_dict["isHidden"] = is_hidden

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name", UNSET)

        is_default = d.pop("isDefault", UNSET)

        is_hidden = d.pop("isHidden", UNSET)

        schema_name = cls(
            name=name,
            is_default=is_default,
            is_hidden=is_hidden,
        )

        schema_name.additional_properties = d
        return schema_name

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
