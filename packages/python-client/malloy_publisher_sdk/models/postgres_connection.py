from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="PostgresConnection")


@_attrs_define
class PostgresConnection:
    """
    Attributes:
        host (Union[Unset, str]):
        port (Union[Unset, int]):
        database_name (Union[Unset, str]):
        user_name (Union[Unset, str]):
        password (Union[Unset, str]):
        connection_string (Union[Unset, str]):
    """

    host: Union[Unset, str] = UNSET
    port: Union[Unset, int] = UNSET
    database_name: Union[Unset, str] = UNSET
    user_name: Union[Unset, str] = UNSET
    password: Union[Unset, str] = UNSET
    connection_string: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        host = self.host

        port = self.port

        database_name = self.database_name

        user_name = self.user_name

        password = self.password

        connection_string = self.connection_string

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if host is not UNSET:
            field_dict["host"] = host
        if port is not UNSET:
            field_dict["port"] = port
        if database_name is not UNSET:
            field_dict["databaseName"] = database_name
        if user_name is not UNSET:
            field_dict["userName"] = user_name
        if password is not UNSET:
            field_dict["password"] = password
        if connection_string is not UNSET:
            field_dict["connectionString"] = connection_string

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        host = d.pop("host", UNSET)

        port = d.pop("port", UNSET)

        database_name = d.pop("databaseName", UNSET)

        user_name = d.pop("userName", UNSET)

        password = d.pop("password", UNSET)

        connection_string = d.pop("connectionString", UNSET)

        postgres_connection = cls(
            host=host,
            port=port,
            database_name=database_name,
            user_name=user_name,
            password=password,
            connection_string=connection_string,
        )

        postgres_connection.additional_properties = d
        return postgres_connection

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
