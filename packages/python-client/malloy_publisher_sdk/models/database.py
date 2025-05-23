from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.database_type import DatabaseType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.table_description import TableDescription


T = TypeVar("T", bound="Database")


@_attrs_define
class Database:
    """An in-memory DuckDB database embedded in the package.

    Attributes:
        resource (Union[Unset, str]): Resource path to the database.
        path (Union[Unset, str]): Database's relative path in its package directory.
        info (Union[Unset, TableDescription]):
        type_ (Union[Unset, DatabaseType]): Type of database.
    """

    resource: Union[Unset, str] = UNSET
    path: Union[Unset, str] = UNSET
    info: Union[Unset, "TableDescription"] = UNSET
    type_: Union[Unset, DatabaseType] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        path = self.path

        info: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.info, Unset):
            info = self.info.to_dict()

        type_: Union[Unset, str] = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if path is not UNSET:
            field_dict["path"] = path
        if info is not UNSET:
            field_dict["info"] = info
        if type_ is not UNSET:
            field_dict["type"] = type_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_description import TableDescription

        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        path = d.pop("path", UNSET)

        _info = d.pop("info", UNSET)
        info: Union[Unset, TableDescription]
        if isinstance(_info, Unset):
            info = UNSET
        else:
            info = TableDescription.from_dict(_info)

        _type_ = d.pop("type", UNSET)
        type_: Union[Unset, DatabaseType]
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = DatabaseType(_type_)

        database = cls(
            resource=resource,
            path=path,
            info=info,
            type_=type_,
        )

        database.additional_properties = d
        return database

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
