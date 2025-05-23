from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.column import Column


T = TypeVar("T", bound="TableSource")


@_attrs_define
class TableSource:
    """
    Attributes:
        resource (Union[Unset, str]): Resource path to the table source.
        source (Union[Unset, str]):
        columns (Union[Unset, list['Column']]): Table fields
    """

    resource: Union[Unset, str] = UNSET
    source: Union[Unset, str] = UNSET
    columns: Union[Unset, list["Column"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        source = self.source

        columns: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.columns, Unset):
            columns = []
            for columns_item_data in self.columns:
                columns_item = columns_item_data.to_dict()
                columns.append(columns_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if source is not UNSET:
            field_dict["source"] = source
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.column import Column

        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        source = d.pop("source", UNSET)

        columns = []
        _columns = d.pop("columns", UNSET)
        for columns_item_data in _columns or []:
            columns_item = Column.from_dict(columns_item_data)

            columns.append(columns_item)

        table_source = cls(
            resource=resource,
            source=source,
            columns=columns,
        )

        table_source.additional_properties = d
        return table_source

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
