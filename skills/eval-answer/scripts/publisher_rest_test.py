#!/usr/bin/env python3
"""Tests for publisher_rest. Stdlib only: python3 publisher_rest_test.py"""
import os
import unittest

from publisher_rest import rows_from_result, uncell


def envelope(fields, records):
    """A query response in the shape Publisher returns."""
    return {"schema": {"fields": [{"name": n} for n in fields]},
            "data": {"array_value": records}}


def record(*cells):
    return {"kind": "record_cell", "record_value": list(cells)}


class UncellTests(unittest.TestCase):
    def test_scalars_by_value_key(self):
        self.assertEqual(uncell({"string_value": "Levi's"}), "Levi's")
        self.assertEqual(uncell({"number_value": 27016}), 27016)
        self.assertEqual(uncell({"boolean_value": False}), False)

    def test_null_cell_is_none(self):
        # No `_value` key at all. The shallow parser this replaced got this
        # right by accident (its default was None); assert it on purpose.
        self.assertIsNone(uncell({"kind": "null_cell"}))

    def test_nested_record_is_walked(self):
        # The case the two implementations disagreed on. The shallow parser
        # returned the raw envelope here, and those rows reached the judge.
        cell = record({"number_value": 2022}, {"number_value": 91})
        self.assertEqual(uncell(cell), {"c0": 2022, "c1": 91})

    def test_nested_array_of_records_is_walked(self):
        cell = {"kind": "array_cell", "array_value": [record({"number_value": 1})]}
        out = uncell(cell)
        self.assertEqual(out, [{"c0": 1}])
        self.assertNotIn("record_value", repr(out))

    def test_unknown_cell_shape_passes_through(self):
        # Reporting a shape we do not know beats inventing None for it.
        self.assertEqual(uncell({"mystery": 1}), {"mystery": 1})


class RowsFromResultTests(unittest.TestCase):
    def test_names_come_from_schema_positionally(self):
        body = envelope(["brand", "n"],
                        [record({"string_value": "Levi's"}, {"number_value": 27016})])
        self.assertEqual(rows_from_result(body), [{"brand": "Levi's", "n": 27016}])

    def test_result_may_be_a_json_string(self):
        import json
        body = envelope(["n"], [record({"number_value": 1})])
        self.assertEqual(rows_from_result({"result": json.dumps(body)}),
                         [{"n": 1}])

    def test_result_may_be_a_plain_list(self):
        self.assertEqual(rows_from_result({"result": [{"a": 1}]}), [{"a": 1}])

    def test_no_rows_is_empty_not_an_error(self):
        # An empty result carries evidence (the query ran, nothing matched);
        # it must not be indistinguishable from a transport failure.
        self.assertEqual(rows_from_result(envelope(["n"], [])), [])

    def test_nest_column_is_usable_rows(self):
        body = envelope(["brand", "by_year"], [
            record({"string_value": "Levi's"},
                   {"kind": "array_cell",
                    "array_value": [record({"number_value": 2022})]})])
        self.assertEqual(rows_from_result(body),
                         [{"brand": "Levi's", "by_year": [{"c0": 2022}]}])


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
