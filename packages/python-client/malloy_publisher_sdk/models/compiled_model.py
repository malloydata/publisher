from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query import Query


T = TypeVar("T", bound="CompiledModel")


@_attrs_define
class CompiledModel:
    """Malloy model def and result data.  Malloy model def and result data is Malloy version depdendent.

    Attributes:
        resource (Union[Unset, str]): Resource path to the model.
        package_name (Union[Unset, str]): Model's package Name
        path (Union[Unset, str]): Model's relative path in its package directory.
        malloy_version (Union[Unset, str]): Version of the Malloy compiler that generated the model def and results
            fields.
        model_info (Union[Unset, str]): JSON string of ModelInfo. See malloy/packages/malloy-interfaces/src/types.ts
        source_infos (Union[Unset, list[str]]): Array of JSON string of SourceInfo. See malloy/packages/malloy-
            interfaces/src/types.ts
        queries (Union[Unset, list['Query']]):
    """

    resource: Union[Unset, str] = UNSET
    package_name: Union[Unset, str] = UNSET
    path: Union[Unset, str] = UNSET
    malloy_version: Union[Unset, str] = UNSET
    model_info: Union[Unset, str] = UNSET
    source_infos: Union[Unset, list[str]] = UNSET
    queries: Union[Unset, list["Query"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        package_name = self.package_name

        path = self.path

        malloy_version = self.malloy_version

        model_info = self.model_info

        source_infos: Union[Unset, list[str]] = UNSET
        if not isinstance(self.source_infos, Unset):
            source_infos = self.source_infos

        queries: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.queries, Unset):
            queries = []
            for queries_item_data in self.queries:
                queries_item = queries_item_data.to_dict()
                queries.append(queries_item)

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
        if model_info is not UNSET:
            field_dict["modelInfo"] = model_info
        if source_infos is not UNSET:
            field_dict["sourceInfos"] = source_infos
        if queries is not UNSET:
            field_dict["queries"] = queries

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query import Query

        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        package_name = d.pop("packageName", UNSET)

        path = d.pop("path", UNSET)

        malloy_version = d.pop("malloyVersion", UNSET)

        model_info = d.pop("modelInfo", UNSET)

        source_infos = cast(list[str], d.pop("sourceInfos", UNSET))

        queries = []
        _queries = d.pop("queries", UNSET)
        for queries_item_data in _queries or []:
            queries_item = Query.from_dict(queries_item_data)

            queries.append(queries_item)

        compiled_model = cls(
            resource=resource,
            package_name=package_name,
            path=path,
            malloy_version=malloy_version,
            model_info=model_info,
            source_infos=source_infos,
            queries=queries,
        )

        compiled_model.additional_properties = d
        return compiled_model

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
