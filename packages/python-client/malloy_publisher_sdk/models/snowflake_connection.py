from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SnowflakeConnection")


@_attrs_define
class SnowflakeConnection:
    """
    Attributes:
        account (Union[Unset, str]):
        username (Union[Unset, str]):
        password (Union[Unset, str]):
        warehouse (Union[Unset, str]):
        database (Union[Unset, str]):
        schema (Union[Unset, str]):
        response_timeout_milliseconds (Union[Unset, int]):
    """

    account: Union[Unset, str] = UNSET
    username: Union[Unset, str] = UNSET
    password: Union[Unset, str] = UNSET
    warehouse: Union[Unset, str] = UNSET
    database: Union[Unset, str] = UNSET
    schema: Union[Unset, str] = UNSET
    response_timeout_milliseconds: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        account = self.account

        username = self.username

        password = self.password

        warehouse = self.warehouse

        database = self.database

        schema = self.schema

        response_timeout_milliseconds = self.response_timeout_milliseconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if account is not UNSET:
            field_dict["account"] = account
        if username is not UNSET:
            field_dict["username"] = username
        if password is not UNSET:
            field_dict["password"] = password
        if warehouse is not UNSET:
            field_dict["warehouse"] = warehouse
        if database is not UNSET:
            field_dict["database"] = database
        if schema is not UNSET:
            field_dict["schema"] = schema
        if response_timeout_milliseconds is not UNSET:
            field_dict["responseTimeoutMilliseconds"] = response_timeout_milliseconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        account = d.pop("account", UNSET)

        username = d.pop("username", UNSET)

        password = d.pop("password", UNSET)

        warehouse = d.pop("warehouse", UNSET)

        database = d.pop("database", UNSET)

        schema = d.pop("schema", UNSET)

        response_timeout_milliseconds = d.pop("responseTimeoutMilliseconds", UNSET)

        snowflake_connection = cls(
            account=account,
            username=username,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            response_timeout_milliseconds=response_timeout_milliseconds,
        )

        snowflake_connection.additional_properties = d
        return snowflake_connection

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
