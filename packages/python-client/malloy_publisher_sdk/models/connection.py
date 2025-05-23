from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connection_type import ConnectionType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.bigquery_connection import BigqueryConnection
    from ..models.connection_attributes import ConnectionAttributes
    from ..models.postgres_connection import PostgresConnection
    from ..models.snowflake_connection import SnowflakeConnection
    from ..models.trino_connection import TrinoConnection


T = TypeVar("T", bound="Connection")


@_attrs_define
class Connection:
    """
    Attributes:
        resource (Union[Unset, str]): Resource path to the connection.
        name (Union[Unset, str]):
        type_ (Union[Unset, ConnectionType]):
        attributes (Union[Unset, ConnectionAttributes]):
        postgres_connection (Union[Unset, PostgresConnection]):
        bigquery_connection (Union[Unset, BigqueryConnection]):
        snowflake_connection (Union[Unset, SnowflakeConnection]):
        trino_connection (Union[Unset, TrinoConnection]):
    """

    resource: Union[Unset, str] = UNSET
    name: Union[Unset, str] = UNSET
    type_: Union[Unset, ConnectionType] = UNSET
    attributes: Union[Unset, "ConnectionAttributes"] = UNSET
    postgres_connection: Union[Unset, "PostgresConnection"] = UNSET
    bigquery_connection: Union[Unset, "BigqueryConnection"] = UNSET
    snowflake_connection: Union[Unset, "SnowflakeConnection"] = UNSET
    trino_connection: Union[Unset, "TrinoConnection"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        name = self.name

        type_: Union[Unset, str] = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        attributes: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.attributes, Unset):
            attributes = self.attributes.to_dict()

        postgres_connection: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.postgres_connection, Unset):
            postgres_connection = self.postgres_connection.to_dict()

        bigquery_connection: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.bigquery_connection, Unset):
            bigquery_connection = self.bigquery_connection.to_dict()

        snowflake_connection: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.snowflake_connection, Unset):
            snowflake_connection = self.snowflake_connection.to_dict()

        trino_connection: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.trino_connection, Unset):
            trino_connection = self.trino_connection.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if name is not UNSET:
            field_dict["name"] = name
        if type_ is not UNSET:
            field_dict["type"] = type_
        if attributes is not UNSET:
            field_dict["attributes"] = attributes
        if postgres_connection is not UNSET:
            field_dict["postgresConnection"] = postgres_connection
        if bigquery_connection is not UNSET:
            field_dict["bigqueryConnection"] = bigquery_connection
        if snowflake_connection is not UNSET:
            field_dict["snowflakeConnection"] = snowflake_connection
        if trino_connection is not UNSET:
            field_dict["trinoConnection"] = trino_connection

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.bigquery_connection import BigqueryConnection
        from ..models.connection_attributes import ConnectionAttributes
        from ..models.postgres_connection import PostgresConnection
        from ..models.snowflake_connection import SnowflakeConnection
        from ..models.trino_connection import TrinoConnection

        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        name = d.pop("name", UNSET)

        _type_ = d.pop("type", UNSET)
        type_: Union[Unset, ConnectionType]
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = ConnectionType(_type_)

        _attributes = d.pop("attributes", UNSET)
        attributes: Union[Unset, ConnectionAttributes]
        if isinstance(_attributes, Unset):
            attributes = UNSET
        else:
            attributes = ConnectionAttributes.from_dict(_attributes)

        _postgres_connection = d.pop("postgresConnection", UNSET)
        postgres_connection: Union[Unset, PostgresConnection]
        if isinstance(_postgres_connection, Unset):
            postgres_connection = UNSET
        else:
            postgres_connection = PostgresConnection.from_dict(_postgres_connection)

        _bigquery_connection = d.pop("bigqueryConnection", UNSET)
        bigquery_connection: Union[Unset, BigqueryConnection]
        if isinstance(_bigquery_connection, Unset):
            bigquery_connection = UNSET
        else:
            bigquery_connection = BigqueryConnection.from_dict(_bigquery_connection)

        _snowflake_connection = d.pop("snowflakeConnection", UNSET)
        snowflake_connection: Union[Unset, SnowflakeConnection]
        if isinstance(_snowflake_connection, Unset):
            snowflake_connection = UNSET
        else:
            snowflake_connection = SnowflakeConnection.from_dict(_snowflake_connection)

        _trino_connection = d.pop("trinoConnection", UNSET)
        trino_connection: Union[Unset, TrinoConnection]
        if isinstance(_trino_connection, Unset):
            trino_connection = UNSET
        else:
            trino_connection = TrinoConnection.from_dict(_trino_connection)

        connection = cls(
            resource=resource,
            name=name,
            type_=type_,
            attributes=attributes,
            postgres_connection=postgres_connection,
            bigquery_connection=bigquery_connection,
            snowflake_connection=snowflake_connection,
            trino_connection=trino_connection,
        )

        connection.additional_properties = d
        return connection

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
