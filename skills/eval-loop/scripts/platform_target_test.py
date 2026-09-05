#!/usr/bin/env python3
"""What `--target platform` selects, without a hosted server.

The live half of a platform run needs that host's MCP URL, an interactive OAuth
login and a published version, so it cannot run in CI. Everything BELOW the
network can, and it is the half that was refactored when the vendor's tool names
became configuration -- which tools are granted, under which server name, which
prompt is used, and whether the answerer was handed skills written for another
host. Each of those fails silently on a real run: the answerer simply reads
instructions for a surface it is not on, and the run still reports a number.

Stdlib only: python3 platform_target_test.py
"""
import argparse
import os
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import run_baseline as rb  # noqa: E402


class HostedToolNames(unittest.TestCase):
    def test_prefix_is_the_server_name(self):
        self.assertEqual(
            rb.hosted_tools("acme", ("get_context", "execute_query")),
            ("mcp__acme__get_context", "mcp__acme__execute_query"))

    def test_default_bare_tools_carry_no_vendor(self):
        # The default must be what any host plausibly exposes. A product's own
        # doc-search tool was a hardcoded constant here once; it is a flag now.
        for t in rb.HOSTED_TOOLS_DEFAULT:
            self.assertNotIn("credible", t.lower())
            self.assertFalse(t.startswith("mcp__"), t)

    def test_a_hosts_extra_tool_can_be_added(self):
        got = rb.hosted_tools("h", ("get_context", "search_h_docs"))
        self.assertIn("mcp__h__search_h_docs", got)

    def test_local_tools_are_publishers_own_and_unprefixed_by_this(self):
        for t in rb.ANSWER_TOOLS:
            self.assertTrue(t.startswith("mcp__publisher__"), t)


class PromptSelection(unittest.TestCase):
    def test_platform_prompt_names_no_vendor(self):
        self.assertNotIn("credible", rb.PLATFORM_PROMPT.lower())

    def test_both_prompts_take_the_same_fields(self):
        # run_answerer formats one or the other with the same kwargs; a field in
        # one and not the other is a KeyError on the arm that uses it.
        import re
        f = lambda s: set(re.findall(r"\{(\w+)\}", s))
        self.assertEqual(f(rb.ANSWER_PROMPT) - {"scope_line"},
                         f(rb.PLATFORM_PROMPT) - {"scope_line"})

    def test_scope_line_pins_one_package(self):
        line = rb.SCOPE_LINE.format(env="e", pkg="p")
        self.assertIn('"environment": "e"', line)
        self.assertIn("Do not query any other package", line)


class SkillsWrittenForAnotherHost(unittest.TestCase):
    """The guard that says so before a platform run measures the wrong thing.

    Over a skills tree this test writes, not the repo's. Naming real skills
    asserted the repo's contents rather than the function's contract: it read
    as a pass while `malloy-getting-started` happened to exist and happened to
    name a `malloy_*` tool, and the same test fails outright in the
    `agent-skills` checkout these skills also ship to, where that
    Publisher-only skill is absent.
    """

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.roots = [self.tmp]
        self.write("host-skill", "Call `malloy_getContext`, then `malloy_executeQuery`.")
        self.write("shared-skill", "Call `get_context`, then `execute_query`.")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, name: str, body: str) -> None:
        d = self.tmp / "skills" / name
        d.mkdir(parents=True)
        (d / "SKILL.md").write_text(f"---\nname: {name}\n---\n\n{body}\n")

    def test_flags_a_publisher_host_skill(self):
        self.assertEqual(rb.publisher_only_skills(["host-skill"], self.roots),
                         ["host-skill"])

    def test_passes_skills_that_use_bare_tool_names(self):
        # Bare tool names are what make a shared skill portable across hosts,
        # and skills/README.md says so.
        self.assertEqual(rb.publisher_only_skills(["shared-skill"], self.roots),
                         [])

    def test_a_missing_skill_is_not_reported_as_publisher_only(self):
        self.assertEqual(rb.publisher_only_skills(["no-such-skill"], self.roots), [])

    def test_a_root_that_is_the_skills_dir_itself_also_resolves(self):
        # publisher_only_skills accepts either a repo root holding skills/ or
        # the skills directory itself; both callers exist.
        self.assertEqual(
            rb.publisher_only_skills(["host-skill"], [self.tmp / "skills"]),
            ["host-skill"])

    def test_every_publisher_only_tool_is_detected(self):
        for i, tool in enumerate(rb.PUBLISHER_ONLY_TOOLS):
            name = f"uses-{i}"
            self.write(name, f"Call `{tool}`.")
            self.assertEqual(rb.publisher_only_skills([name], self.roots),
                             [name], tool)


class HostedToolsResolveOnce(unittest.TestCase):
    def test_comma_list_becomes_prefixed_tuple(self):
        # main() resolves this once at setup: the granted tool list is part of
        # what a run measured, so it must not vary within a run.
        a = argparse.Namespace(hosted_mcp_server="h", hosted_tools="get_context, execute_query")
        a.hosted_tools = rb.hosted_tools(
            a.hosted_mcp_server,
            tuple(s.strip() for s in a.hosted_tools.split(",") if s.strip()))
        self.assertEqual(a.hosted_tools,
                         ("mcp__h__get_context", "mcp__h__execute_query"))


if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    unittest.main()
