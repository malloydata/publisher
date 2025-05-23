from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="Project")


@_attrs_define
class Project:
    """
    Attributes:
        resource (Union[Unset, str]): Resource path to the project.
        name (Union[Unset, str]): Project name.
        readme (Union[Unset, str]): Project readme.
    """

    resource: Union[Unset, str] = UNSET
    name: Union[Unset, str] = UNSET
    readme: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        name = self.name

        readme = self.readme

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if name is not UNSET:
            field_dict["name"] = name
        if readme is not UNSET:
            field_dict["readme"] = readme

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        name = d.pop("name", UNSET)

        readme = d.pop("readme", UNSET)

        project = cls(
            resource=resource,
            name=name,
            readme=readme,
        )

        project.additional_properties = d
        return project

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
