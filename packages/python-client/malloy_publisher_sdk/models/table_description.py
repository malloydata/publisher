from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.column import Column


T = TypeVar("T", bound="TableDescription")


@_attrs_define
class TableDescription:
    """
    Attributes:
        name (Union[Unset, str]):
        row_count (Union[Unset, int]):
        columns (Union[Unset, list['Column']]):
    """

    name: Union[Unset, str] = UNSET
    row_count: Union[Unset, int] = UNSET
    columns: Union[Unset, list["Column"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        row_count = self.row_count

        columns: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.columns, Unset):
            columns = []
            for columns_item_data in self.columns:
                columns_item = columns_item_data.to_dict()
                columns.append(columns_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if row_count is not UNSET:
            field_dict["rowCount"] = row_count
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.column import Column

        d = dict(src_dict)
        name = d.pop("name", UNSET)

        row_count = d.pop("rowCount", UNSET)

        columns = []
        _columns = d.pop("columns", UNSET)
        for columns_item_data in _columns or []:
            columns_item = Column.from_dict(columns_item_data)

            columns.append(columns_item)

        table_description = cls(
            name=name,
            row_count=row_count,
            columns=columns,
        )

        table_description.additional_properties = d
        return table_description

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
