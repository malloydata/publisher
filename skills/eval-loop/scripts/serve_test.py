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
import pathlib
import sys
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main(verbosity=1)
