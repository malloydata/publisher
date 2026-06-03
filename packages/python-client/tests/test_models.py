import pytest
from malloy_publisher_sdk.models import Environment  # Generated model
from malloy_publisher_sdk.types import UNSET  # Import UNSET


def test_environment_model_instantiation_and_attributes():
    """Test attrs model instantiation and attribute access for Environment."""
    # Valid data
    valid_data = {
        "resource": "/environments/my-environment",
        "name": "my-environment",
        "readme": "This is a test environment.",
    }
    environment = Environment(**valid_data)
    assert environment.name == "my-environment"
    assert environment.resource == "/environments/my-environment"
    assert environment.readme == "This is a test environment."

    # Test with optional fields being UNSET by default or if not provided
    # (assuming fields are defined with `Union[Unset, str] = UNSET`)
    environment_minimal = Environment()
    assert environment_minimal.resource is UNSET
    assert environment_minimal.name is UNSET
    assert environment_minimal.readme is UNSET

    environment_partial = Environment(name="partial-environment")
    assert environment_partial.name == "partial-environment"
    assert environment_partial.resource is UNSET

    # Type checking with attrs usually relies on runtime checks if converters are used,
    # or by static analysis if type hints are precise. Basic attrs classes are more permissive.
    # For example, assigning an int to a str-hinted field won't raise an error on instantiation
    # unless a validator or converter is in place.
    # So, we can't use pytest.raises(ValidationError) in the same way as Pydantic.

    # Example of how you might expect a type error if you try to use it incorrectly later:
    with pytest.raises(TypeError):
        # This will cause a TypeError if an operation expects name to be a string
        # and it was set to an int, and that operation is type sensitive.
        # This is a weak test for attrs without explicit validators.
        Environment(name=123).name + "_suffix"  # type: ignore

    # Test additional_properties if present and relevant
    if hasattr(environment, "additional_properties"):
        # For attrs, additional properties are typically handled by the from_dict classmethod
        # or by direct assignment to the .additional_properties attribute after instantiation.
        # Direct instantiation with unexpected kwargs will raise a TypeError.

        # Test assignment after instantiation
        environment_with_extras_assigned = Environment(name="extras_assigned")
        environment_with_extras_assigned.additional_properties[
            "new_field_assigned"
        ] = "new_value_assigned"
        assert (
            environment_with_extras_assigned.additional_properties[
                "new_field_assigned"
            ]
            == "new_value_assigned"
        )
        assert "new_field_assigned" in environment_with_extras_assigned
        assert (
            environment_with_extras_assigned["new_field_assigned"]
            == "new_value_assigned"
        )

        # Test from_dict pathway for additional properties
        data_for_from_dict = {
            "name": "extras_from_dict",
            "unknown_field": "some_value",
        }
        environment_from_dict = Environment.from_dict(data_for_from_dict)
        assert environment_from_dict.name == "extras_from_dict"
        assert (
            environment_from_dict.additional_properties["unknown_field"]
            == "some_value"
        )
        assert "unknown_field" in environment_from_dict
        assert environment_from_dict["unknown_field"] == "some_value"
