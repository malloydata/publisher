#!/usr/bin/env python3
"""Tests for publisher_rest. Stdlib only: python3 publisher_rest_test.py"""
import json
import os
import unittest
import unittest.mock

import publisher_rest
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


class CompactJson(unittest.TestCase):
    """The request asks for compact rows, which is what recovers the names
    inside a nest.

    The typed envelope does not repeat field names below the top level, so a
    nested record came back keyed `c0`, `c1`. A golden written the way its
    author ran the query -- with the real column names -- then read as DRIFT,
    and drift blocks an arm. Publisher has answered this all along; the
    request just never asked.
    """

    def send(self, body):
        """Capture the request `query()` builds, and hand back `body`."""
        seen = {}

        class Resp:
            def read(self_): return json.dumps(body).encode()
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False

        def fake(req, timeout=None):
            seen["payload"] = json.loads(req.data.decode())
            return Resp()

        with unittest.mock.patch.object(publisher_rest.urllib.request,
                                        "urlopen", fake):
            rows = publisher_rest.query("http://x", "e", "p", "m.malloy",
                                        "run: x -> { ... }")
        return seen["payload"], rows

    def test_the_request_asks_for_compact_rows(self):
        payload, _ = self.send({"result": "[]"})
        self.assertIs(payload["compactJson"], True)
        self.assertEqual(payload["query"], "run: x -> { ... }")

    def test_a_nest_keeps_its_field_names(self):
        # What the server returns under compactJson: `result` is a JSON
        # STRING of plain rows, nested names intact.
        body = {"result": json.dumps(
            [{"carrier": "WN", "n": 2,
              "top_dest": [{"destination_code": "PHX", "d": 1}]}])}
        _, rows = self.send(body)
        self.assertEqual(rows[0]["top_dest"], [{"destination_code": "PHX",
                                                "d": 1}])
        self.assertNotIn("c0", rows[0]["top_dest"][0])

    def test_the_envelope_path_still_works(self):
        # Kept as a fallback: a server that ignores the flag still parses.
        body = envelope(["n"], [record({"number_value": 7})])
        _, rows = self.send(body)
        self.assertEqual(rows, [{"n": 7}])


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
