#!/usr/bin/env python3
"""Tests for when serve.py seeds the store and when it preserves it.

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
