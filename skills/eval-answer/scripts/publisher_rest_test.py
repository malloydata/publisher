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


class PackageVersionTests(unittest.TestCase):
    """Read off GET package (versionId, latestVersion) and GET versions/{id}."""

    def read(self, responses):
        def fake(base, path, timeout=30):
            r = responses.get(path)
            if isinstance(r, Exception):
                raise r
            return r
        with unittest.mock.patch.object(publisher_rest, "get_json", side_effect=fake):
            return publisher_rest.package_version("http://x", "e", "p")

    def test_published_latest_with_its_hash(self):
        got = self.read({
            "api/v0/environments/e/packages/p": {"versionId": "1.2.0", "latestVersion": "1.2.0"},
            "api/v0/environments/e/packages/p/versions/1.2.0":
                {"id": "1.2.0", "latest": True, "contentHash": "h", "archiveStatus": "unarchive"}})
        self.assertEqual(got, {"id": "1.2.0", "latest": True, "contentHash": "h", "archived": False})

    def test_unversioned_package_has_no_id(self):
        self.assertEqual(self.read({"api/v0/environments/e/packages/p": {"name": "p"}}), {"id": None})

    def test_unreachable_server_is_empty(self):
        self.assertEqual(self.read({"api/v0/environments/e/packages/p": OSError("refused")}), {})

    def test_a_failed_version_read_keeps_the_id(self):
        got = self.read({
            "api/v0/environments/e/packages/p": {"versionId": "1.2.0", "latestVersion": "1.3.0"},
            "api/v0/environments/e/packages/p/versions/1.2.0": OSError("404")})
        self.assertEqual(got, {"id": "1.2.0", "latest": False, "contentHash": None, "archived": None})


class ServedPackageDirTests(unittest.TestCase):
    """A published version lives in <package>/<version>/; latest's is the one served."""

    def setUp(self):
        import tempfile, pathlib
        self.root = pathlib.Path(tempfile.mkdtemp())
        (self.root / "p" / "1.0.1").mkdir(parents=True)
        (self.root / "p" / "1.0.1" / "m.malloy").write_text("source: s is x")

    def resolve(self, version_id):
        def fake(base, path, timeout=30):
            if path == "api/v0/projects":
                return [{"name": "e", "location": str(self.root)}]
            if path == "api/v0/environments/e/packages/p":
                return {"versionId": version_id, "latestVersion": version_id}
            raise OSError(path)
        with unittest.mock.patch.object(publisher_rest, "get_json", side_effect=fake):
            return (publisher_rest.served_package_dir("http://x", "e", "p"),
                    publisher_rest.served_model_path("http://x", "e", "p", "m.malloy"))

    def test_a_published_package_resolves_to_latest_s_directory(self):
        d, m = self.resolve("1.0.1")
        self.assertEqual(d, self.root / "p" / "1.0.1")
        self.assertEqual(m, self.root / "p" / "1.0.1" / "m.malloy")

    def test_an_unversioned_package_is_its_own_directory(self):
        d, m = self.resolve(None)
        self.assertEqual(d, self.root / "p")
        self.assertIsNone(m)


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

    def test_givens_travel_in_the_request_body(self):
        seen = {}

        class Resp:
            def read(self_): return b'{"result": "[]"}'
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False

        def fake(req, timeout=None):
            seen["payload"] = json.loads(req.data.decode())
            return Resp()

        with unittest.mock.patch.object(publisher_rest.urllib.request,
                                        "urlopen", fake):
            publisher_rest.query("http://x", "e", "p", "m.malloy", "run: x",
                                 givens={"region": "West"})
            self.assertEqual(seen["payload"]["givens"], {"region": "West"})
            publisher_rest.query("http://x", "e", "p", "m.malloy", "run: x")
            self.assertNotIn("givens", seen["payload"])

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



class PackageIdentityTests(unittest.TestCase):
    """The served identity comes from the package's serving state, and on older
    servers from its `/status` entry or the package resource."""

    def identity(self, responses):
        def get_json(base, path):
            if path in responses:
                return responses[path]
            raise OSError("HTTP Error 404")
        with unittest.mock.patch.object(publisher_rest, "get_json", get_json):
            return publisher_rest.package_identity("http://x", "env", "pkg")

    def test_the_serving_state_answers(self):
        state = {"name": "pkg", "sourceContentSha": "abc", "servedRevision": "r1"}
        self.assertEqual(
            self.identity({"api/v0/environments/env/packages/pkg/status": state}),
            {"sourceContentSha": "abc", "servedRevision": "r1"})

    def test_a_server_without_the_route_answers_from_its_status_entry(self):
        status = {"environments": [{"name": "env", "packages": [
            {"name": "other", "sourceContentSha": "zzz"},
            {"name": "pkg", "sourceContentSha": "abc", "servedRevision": "r1"},
        ]}]}
        self.assertEqual(self.identity({"api/v0/status": status}),
                         {"sourceContentSha": "abc", "servedRevision": "r1"})

    def test_a_versioned_package_answers_from_its_latest_version(self):
        status = {"environments": [{"name": "env", "packages": [
            {"name": "pkg", "versionId": "1.0.0", "latestVersion": "1.1.0",
             "sourceContentSha": "old"},
            {"name": "pkg", "versionId": "1.1.0", "latestVersion": "1.1.0",
             "sourceContentSha": "new"},
        ]}]}
        self.assertEqual(self.identity({"api/v0/status": status}),
                         {"sourceContentSha": "new"})

    def test_an_older_server_answers_from_the_package_resource(self):
        status = {"environments": [{"name": "env", "packages": [{"name": "pkg"}]}]}
        pkg = {"sourceContentSha": "abc", "servedRevision": "r1"}
        self.assertEqual(
            self.identity({"api/v0/status": status,
                           "api/v0/environments/env/packages/pkg": pkg}),
            {"sourceContentSha": "abc", "servedRevision": "r1"})

class BuildCosts(unittest.TestCase):
    def test_one_row_per_built_table_with_what_the_server_reported(self):
        manifest = {"entries": {
            "b@m": {"sourceName": "b", "rowCount": 10, "buildDurationMs": 400,
                    "queryCostBytes": None, "storageDestinationName": "lake"},
            "a@m": {"sourceName": "a", "origin": "preaggregate", "rowCount": 3,
                    "buildDurationMs": None, "queryCostBytes": 2048},
            "bad@m": {"sourceName": "bad", "error": "boom"}}}
        got = publisher_rest.build_costs(manifest)
        self.assertEqual([b["source"] for b in got], ["a", "b"])       # sorted, failures out
        self.assertEqual((got[0]["origin"], got[0]["buildMs"], got[0]["scannedBytes"]),
                         ("preaggregate", None, 2048))                  # null stays null
        self.assertEqual((got[1]["origin"], got[1]["rows"], got[1]["storage"]),
                         ("persist", 10, "lake"))

    def test_no_manifest_is_no_builds(self):
        for m in (None, {}, {"entries": None}, "x"):
            self.assertEqual(publisher_rest.build_costs(m), [])


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
