from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.notebook_cell_type import NotebookCellType
from ..types import UNSET, Unset

T = TypeVar("T", bound="NotebookCell")


@_attrs_define
class NotebookCell:
    """Notebook cell.

    Attributes:
        type_ (Union[Unset, NotebookCellType]): Type of notebook cell.
        text (Union[Unset, str]): Text contents of the notebook cell.
        result (Union[Unset, str]): JSON string of Malloy.Result. See malloy/packages/malloy-interfaces/src/types.ts
        new_sources (Union[Unset, list[str]]): Array of JSON string of SourceInfo made available in the notebook cell.
            Only *new* SourceInfos are returned. The complete list of SourceInfos is available be concatenating the prior
            notebook cells.  The SourceInfos are in the order they are made available in the notebook cell.
    """

    type_: Union[Unset, NotebookCellType] = UNSET
    text: Union[Unset, str] = UNSET
    result: Union[Unset, str] = UNSET
    new_sources: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: Union[Unset, str] = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        text = self.text

        result = self.result

        new_sources: Union[Unset, list[str]] = UNSET
        if not isinstance(self.new_sources, Unset):
            new_sources = self.new_sources

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if text is not UNSET:
            field_dict["text"] = text
        if result is not UNSET:
            field_dict["result"] = result
        if new_sources is not UNSET:
            field_dict["newSources"] = new_sources

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: Union[Unset, NotebookCellType]
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = NotebookCellType(_type_)

        text = d.pop("text", UNSET)

        result = d.pop("result", UNSET)

        new_sources = cast(list[str], d.pop("newSources", UNSET))

        notebook_cell = cls(
            type_=type_,
            text=text,
            result=result,
            new_sources=new_sources,
        )

        notebook_cell.additional_properties = d
        return notebook_cell

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
