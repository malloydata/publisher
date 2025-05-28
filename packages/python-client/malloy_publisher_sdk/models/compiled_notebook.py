from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.notebook_cell import NotebookCell


T = TypeVar("T", bound="CompiledNotebook")


@_attrs_define
class CompiledNotebook:
    """Malloy notebook def and result data.  Malloy notebook def and result data is Malloy version depdendent.

    Attributes:
        resource (Union[Unset, str]): Resource path to the notebook.
        package_name (Union[Unset, str]): Notebook's package Name
        path (Union[Unset, str]): Notebook's relative path in its package directory.
        malloy_version (Union[Unset, str]): Version of the Malloy compiler that generated the notebook def and results
            fields.
        notebook_cells (Union[Unset, list['NotebookCell']]): Array of notebook cells.
    """

    resource: Union[Unset, str] = UNSET
    package_name: Union[Unset, str] = UNSET
    path: Union[Unset, str] = UNSET
    malloy_version: Union[Unset, str] = UNSET
    notebook_cells: Union[Unset, list["NotebookCell"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        package_name = self.package_name

        path = self.path

        malloy_version = self.malloy_version

        notebook_cells: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.notebook_cells, Unset):
            notebook_cells = []
            for notebook_cells_item_data in self.notebook_cells:
                notebook_cells_item = notebook_cells_item_data.to_dict()
                notebook_cells.append(notebook_cells_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if package_name is not UNSET:
            field_dict["packageName"] = package_name
        if path is not UNSET:
            field_dict["path"] = path
        if malloy_version is not UNSET:
            field_dict["malloyVersion"] = malloy_version
        if notebook_cells is not UNSET:
            field_dict["notebookCells"] = notebook_cells

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.notebook_cell import NotebookCell

        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        package_name = d.pop("packageName", UNSET)

        path = d.pop("path", UNSET)

        malloy_version = d.pop("malloyVersion", UNSET)

        notebook_cells = []
        _notebook_cells = d.pop("notebookCells", UNSET)
        for notebook_cells_item_data in _notebook_cells or []:
            notebook_cells_item = NotebookCell.from_dict(notebook_cells_item_data)

            notebook_cells.append(notebook_cells_item)

        compiled_notebook = cls(
            resource=resource,
            package_name=package_name,
            path=path,
            malloy_version=malloy_version,
            notebook_cells=notebook_cells,
        )

        compiled_notebook.additional_properties = d
        return compiled_notebook

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
