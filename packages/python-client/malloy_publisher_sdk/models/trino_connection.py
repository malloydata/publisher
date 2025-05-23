from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TrinoConnection")


@_attrs_define
class TrinoConnection:
    """
    Attributes:
        server (Union[Unset, str]):
        port (Union[Unset, float]):
        catalog (Union[Unset, str]):
        schema (Union[Unset, str]):
        user (Union[Unset, str]):
        password (Union[Unset, str]):
    """

    server: Union[Unset, str] = UNSET
    port: Union[Unset, float] = UNSET
    catalog: Union[Unset, str] = UNSET
    schema: Union[Unset, str] = UNSET
    user: Union[Unset, str] = UNSET
    password: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        server = self.server

        port = self.port

        catalog = self.catalog

        schema = self.schema

        user = self.user

        password = self.password

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if server is not UNSET:
            field_dict["server"] = server
        if port is not UNSET:
            field_dict["port"] = port
        if catalog is not UNSET:
            field_dict["catalog"] = catalog
        if schema is not UNSET:
            field_dict["schema"] = schema
        if user is not UNSET:
            field_dict["user"] = user
        if password is not UNSET:
            field_dict["password"] = password

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        server = d.pop("server", UNSET)

        port = d.pop("port", UNSET)

        catalog = d.pop("catalog", UNSET)

        schema = d.pop("schema", UNSET)

        user = d.pop("user", UNSET)

        password = d.pop("password", UNSET)

        trino_connection = cls(
            server=server,
            port=port,
            catalog=catalog,
            schema=schema,
            user=user,
            password=password,
        )

        trino_connection.additional_properties = d
        return trino_connection

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
