#!/usr/bin/env python3
"""Tests for serve.py: when it seeds the store, and how it warms retrieval.

`--init` used to be passed on every start. It sets `force=true` on
`initializeSchema`, whose `dropAllTables` list includes `entity_embeddings`, so
each restart wiped the semantic retrieval index. The sync that rebuilds it is
lazy and non-blocking, so `get_context` calls arriving during the rebuild fall
back to lexical retrieval without recording that they did -- a measurement
error, not merely a slow start.

It cannot just be dropped: `--init` is also what makes the server read
`publisher.config.json` rather than the database, so a config edit does not
take without it. Both halves are pinned here.
"""
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import serve  # noqa: E402

SERVER = pathlib.Path("/p/packages/server/dist/server.mjs")


class InitDecision(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="serve-test-"))

    def db(self):
        (self.root / serve.DB_NAME).write_text("")

    def test_a_fresh_root_is_seeded(self):
        seed, why = serve.init_decision(self.root, reinit=False)
        self.assertTrue(seed)
        self.assertIn("fresh server root", why)

    def test_an_existing_store_is_preserved(self):
        self.db()
        seed, why = serve.init_decision(self.root, reinit=False)
        self.assertFalse(seed)
        self.assertIn("preserving", why)

    def test_preserving_names_the_flag_that_rereads_the_config(self):
        # The one silent failure this replaces: an edited publisher.config.json
        # does nothing on a preserved root, because reading it is --init's
        # other job. The line has to say so or nobody can find out why.
        self.db()
        _, why = serve.init_decision(self.root, reinit=False)
        self.assertIn("--reinit", why)
        self.assertIn("publisher.config.json", why)

    def test_reinit_seeds_an_existing_store(self):
        self.db()
        seed, why = serve.init_decision(self.root, reinit=True)
        self.assertTrue(seed)
        self.assertIn("--reinit", why)

    def test_reinit_on_a_fresh_root_is_not_reported_as_a_drop(self):
        # Nothing to drop, so the line should not claim one.
        seed, why = serve.init_decision(self.root, reinit=True)
        self.assertTrue(seed)
        self.assertIn("fresh server root", why)

    def test_every_decision_explains_itself(self):
        for reset in (False, True):
            for existing in (False, True):
                with self.subTest(reinit=reset, existing=existing):
                    r = pathlib.Path(tempfile.mkdtemp(prefix="serve-test-"))
                    if existing:
                        (r / serve.DB_NAME).write_text("")
                    _, why = serve.init_decision(r, reinit=reset)
                    self.assertTrue(why.strip())


class WarmArguments(unittest.TestCase):
    """The warm-up call has to RANK, and has to be unscoped.

    Both are easy to get wrong in a way that still looks like it worked. An
    earlier version passed `search_text` at the TOP level, which is not a
    parameter of the tool at all: the server ignored it, logged
    `hasQuery:false`, never reached the ranking path, and kicked no sync. It
    read as a 300-second timeout.
    """

    def args(self):
        return serve.warm_arguments("examples", "storefront")

    def test_it_ranks_rather_than_enumerating(self):
        # A target with no search_text enumerates; enumeration embeds nothing,
        # so a warm-up built from one reports success having done no work.
        targets = self.args()["search_targets"]
        self.assertTrue(targets)
        for t in targets:
            self.assertTrue(t.get("search_text"))

    def test_the_search_text_is_nested_in_a_target(self):
        # The own-goal above: not a top-level key.
        self.assertNotIn("search_text", self.args())
        self.assertNotIn("query", self.args())

    def test_it_names_exactly_one_scope(self):
        scopes = self.args()["scopes"]
        self.assertEqual(len(scopes), 1)
        self.assertEqual(scopes[0]["environment"], "examples")
        self.assertEqual(scopes[0]["package"], "storefront")

    def test_the_scope_narrows_nothing(self):
        # A narrowed scope is what the sync used to be handed as its desired
        # row set, and it deleted everything outside it.
        for key in ("source", "model_path", "entity_name"):
            self.assertNotIn(key, self.args()["scopes"][0])


class IndexStatus(unittest.TestCase):
    def test_it_reads_the_status(self):
        self.assertEqual(
            serve.index_status({"embeddingIndex": {"status": "ready"}}), "ready")

    def test_a_package_without_an_index_is_not_indexing(self):
        # No provider configured is a different fact from "still working", and
        # waiting on it would hang until the deadline for no reason.
        self.assertIsNone(serve.index_status({}))
        self.assertIsNone(serve.index_status({"embeddingIndex": None}))

    def test_only_indexing_is_non_terminal(self):
        self.assertNotIn("indexing", serve.TERMINAL_INDEX_STATES)
        for state in ("ready", "cooldown", "oversize"):
            self.assertIn(state, serve.TERMINAL_INDEX_STATES)


class WarmRetrievalSeparatesFailureFromAbsence(unittest.TestCase):
    """A transport failure is not a verdict on the server's capability.

    `except: status = None` handed None a second meaning `index_status` is
    written not to have -- "no embedding provider configured" -- so a 404 from
    a mistyped env or package reported as a capability finding. And the caller
    RETURNED on the first failure, inside a poll loop whose whole premise is
    that the server may not be answering cleanly yet.
    """

    def warm(self, responses, wait=30):
        """Run warm_retrieval over a scripted sequence of urlopen outcomes."""
        it = iter(responses)

        class Resp:
            def __init__(self, payload):
                self.payload = payload

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def read(self):
                return json.dumps(self.payload).encode()

        def urlopen(req, timeout=None):
            r = next(it)
            if isinstance(r, Exception):
                raise r
            return Resp(r)

        with mock.patch.object(serve, "_mcp_call", lambda *a, **k: None), \
             mock.patch.object(serve.urllib.request, "urlopen", urlopen), \
             mock.patch.object(serve.time, "sleep", lambda _s: None):
            return serve.warm_retrieval(4811, 4040, "e", "p", wait=wait)

    def test_an_early_failure_is_retried_rather_than_ending_the_warm_up(self):
        # The expected case at startup, not the exotic one: a 503, then the
        # server answers. Returning on the first one ended the warm-up.
        status, line = self.warm([OSError("connection refused"),
                                  {"embeddingIndex": {"status": "ready"}}])
        self.assertEqual(status, "ready")
        self.assertIn("semantic", line)

    def test_a_failure_that_never_clears_is_not_a_capability_verdict(self):
        status, line = self.warm([OSError("HTTP Error 404: Not Found")] * 4000,
                                 wait=0.2)
        self.assertIsNone(status)
        self.assertNotIn("no embedding provider", line)
        self.assertIn("could not read", line)
        # The actionable part: a 404 here is a typo, not a missing provider.
        self.assertIn("environment and package names", line)

    def test_a_parsed_payload_with_no_index_still_reports_absence(self):
        # The meaning `index_status` DOES have must survive the separation.
        status, line = self.warm([{"someOtherField": 1}])
        self.assertIsNone(status)
        self.assertIn("no embedding provider", line)


class ServerCmd(unittest.TestCase):
    def test_seeding_passes_init(self):
        cmd = serve.server_cmd(SERVER, pathlib.Path("/root"), 4811, 4040,
                               seed=True)
        self.assertIn("--init", cmd)

    def test_preserving_does_not_pass_init(self):
        cmd = serve.server_cmd(SERVER, pathlib.Path("/root"), 4811, 4040,
                               seed=False)
        self.assertNotIn("--init", cmd)

    def test_the_ports_and_root_are_passed_either_way(self):
        for seed in (True, False):
            with self.subTest(seed=seed):
                cmd = serve.server_cmd(SERVER, pathlib.Path("/root"), 4811,
                                       4040, seed=seed)
                self.assertEqual(cmd[cmd.index("--server_root") + 1], "/root")
                self.assertEqual(cmd[cmd.index("--port") + 1], "4811")
                self.assertEqual(cmd[cmd.index("--mcp_port") + 1], "4040")

    def test_a_second_server_gets_its_own_mcp_port(self):
        # Two Publishers on one machine (the model under test and the truth
        # server) collide on the default MCP port otherwise.
        cmd = serve.server_cmd(SERVER, pathlib.Path("/r2"), 4812, 4102,
                               seed=False)
        self.assertEqual(cmd[cmd.index("--mcp_port") + 1], "4102")


def a_set(toml: str, truth_package: bool = True) -> pathlib.Path:
    d = pathlib.Path(tempfile.mkdtemp(prefix="serve-set-"))
    (d / "set.json").write_text(json.dumps({"name": "s", "truthPackage": "s-truth"}))
    (d / "eval.toml").write_text(toml)
    (d / "pkg").mkdir()
    if truth_package:
        (d / "truth-package").mkdir()
    return d.resolve()


TOML = """
[model]
environment = "examples"
package = "storefront"
repo = "pkg"
port = 4000
mcp_port = 4040
[truth]
port = 4881
mcp_port = 4882
"""


class Roles(unittest.TestCase):
    def test_the_model_role_serves_the_model_package(self):
        d = a_set(TOML)
        got = serve.role_config(serve.config.load(d), "model")
        self.assertEqual(got, {"frozenConfig": False, "environments": [
            {"name": "examples", "connections": [], "packages": [
                {"name": "storefront", "location": str((d / "pkg").resolve())}]}]})

    def test_the_truth_role_serves_only_the_truth_package(self):
        # This is the heredoc the tour README used to have people paste.
        d = a_set(TOML)
        got = serve.role_config(serve.config.load(d), "truth")
        self.assertEqual(got, {"frozenConfig": False, "environments": [
            {"name": "truth", "connections": [], "packages": [
                {"name": "s-truth",
                 "location": str((d / "truth-package").resolve())}]}]})

    def test_a_truth_package_inside_the_model_package_is_refused_by_both_roles(self):
        # The leak is the MODEL server serving the truth package's .malloy, so
        # refusing only the truth role would still leave it open.
        d = a_set(TOML + 'package_dir = "pkg/evals/truth"\n')
        (d / "pkg" / "evals" / "truth").mkdir(parents=True)
        for role in ("model", "truth"):
            with self.subTest(role=role):
                with self.assertRaises(SystemExit) as e:
                    serve.role_config(serve.config.load(d), role)
                self.assertIn("the model server serves it to the answerer",
                              str(e.exception))

    def test_a_role_on_the_other_roles_port_is_refused(self):
        cfg = serve.config.load(a_set(TOML))
        err = serve.port_clash(cfg, "truth", 4000, 4882)
        self.assertIn("which the model server uses", err)
        self.assertIsNone(serve.port_clash(cfg, "truth", 4881, 4882))

    def test_a_changed_config_forces_init(self):
        root = pathlib.Path(tempfile.mkdtemp(prefix="serve-root-"))
        self.assertFalse(serve.write_config(root, {"a": 1}))   # first write
        self.assertFalse(serve.write_config(root, {"a": 1}))   # unchanged
        self.assertTrue(serve.write_config(root, {"a": 2}))
        (root / serve.DB_NAME).write_text("")
        seed, why = serve.init_decision(root, reinit=False, config_changed=True)
        self.assertTrue(seed)
        self.assertIn("publisher.config.json changed", why)

    def test_a_relative_publisher_dir_is_resolved_before_use(self):
        root = pathlib.Path(tempfile.mkdtemp(prefix="serve-root-"))
        with mock.patch.object(serve, "alive", return_value=False):
            with self.assertRaises(SystemExit) as e:
                serve.main(["--server-root", str(root),
                            "--publisher-dir", "no/such/server"])
        self.assertIn(str(pathlib.Path("no/such/server").resolve()),
                      str(e.exception))


if __name__ == "__main__":
    unittest.main(verbosity=1)
