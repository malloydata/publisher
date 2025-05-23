from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="Schedule")


@_attrs_define
class Schedule:
    """A scheduled task.

    Attributes:
        resource (Union[Unset, str]): Resource in the package that the schedule is attached to.
        schedule (Union[Unset, str]): Schedule (cron format) for executing task.
        action (Union[Unset, str]): Action to execute.
        connection (Union[Unset, str]): Connection to perform action on.
        last_run_time (Union[Unset, float]): Timestamp in milliseconds of the last run.
        last_run_status (Union[Unset, str]): Status of the last run.
    """

    resource: Union[Unset, str] = UNSET
    schedule: Union[Unset, str] = UNSET
    action: Union[Unset, str] = UNSET
    connection: Union[Unset, str] = UNSET
    last_run_time: Union[Unset, float] = UNSET
    last_run_status: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        schedule = self.schedule

        action = self.action

        connection = self.connection

        last_run_time = self.last_run_time

        last_run_status = self.last_run_status

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if schedule is not UNSET:
            field_dict["schedule"] = schedule
        if action is not UNSET:
            field_dict["action"] = action
        if connection is not UNSET:
            field_dict["connection"] = connection
        if last_run_time is not UNSET:
            field_dict["lastRunTime"] = last_run_time
        if last_run_status is not UNSET:
            field_dict["lastRunStatus"] = last_run_status

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        resource = d.pop("resource", UNSET)

        schedule = d.pop("schedule", UNSET)

        action = d.pop("action", UNSET)

        connection = d.pop("connection", UNSET)

        last_run_time = d.pop("lastRunTime", UNSET)

        last_run_status = d.pop("lastRunStatus", UNSET)

        schedule = cls(
            resource=resource,
            schedule=schedule,
            action=action,
            connection=connection,
            last_run_time=last_run_time,
            last_run_status=last_run_status,
        )

        schedule.additional_properties = d
        return schedule

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
