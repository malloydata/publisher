#!/usr/bin/env python3
"""Tests for the fence every spawned eval agent runs behind.

Two things go wrong here silently, and both went wrong: a tool the agent
should not hold, and an MCP server it should not reach. Neither shows up in a
transcript as an error, so neither is caught by anything except an assertion
about the command that was built. `check_contamination.py` reads the ANSWERER's
transcript, so a judge or a clustering agent that reached an outside oracle
leaves nothing behind that anyone looks at.
"""
import argparse
import pathlib
import sys
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "eval-answer" / "scripts"))
import agent_harness as ah  # noqa: E402
import run_baseline as rb  # noqa: E402

SKILLS_ROOT = HERE.parent.parent


def spawn_cmd(**kw) -> list[str]:
    """The argv `spawn_agent` would exec, without execing it."""
    with mock.patch.object(ah, "run_cli",
                           return_value=([], "{}", "", 1, 0.0)) as run:
        ah.spawn_agent("q", skills=["eval-judge"], skills_root=SKILLS_ROOT,
                       model="sonnet", **kw)
    return run.call_args.args[0]


def claude_cmd(**kw) -> list[str]:
    """The argv `run_baseline.claude` would exec."""
    with mock.patch.object(rb, "run_cli",
                           return_value=([], "", "", 1, 0.0)) as run:
        rb.claude("q", str(HERE), "sonnet", **kw)
    return run.call_args.args[0]


class StrictMcpConfig(unittest.TestCase):
    """`--strict-mcp-config` is what confines an agent to the servers named on
    its own command line. Gating it on there BEING a config read "no config" as
    "no restriction", and the agents called with no MCP at all -- the judge,
    the clustering agent, the coverage judge -- inherited the operator's
    account-level connectors: a live `execute_query` against real workspaces,
    plus Gmail send and Drive share."""

    def test_spawn_with_no_mcp_still_restricts(self):
        cmd = spawn_cmd(mcp_url=None)
        self.assertIn("--strict-mcp-config", cmd)
        self.assertNotIn("--mcp-config", cmd)

    def test_spawn_with_an_mcp_url_names_its_config(self):
        cmd = spawn_cmd(mcp_url="http://localhost:4040/mcp")
        self.assertIn("--strict-mcp-config", cmd)
        self.assertIn("--mcp-config", cmd)

    def test_the_judge_arm_restricts(self):
        # run_baseline.py calls the judge with mcp=None explicitly.
        cmd = claude_cmd(mcp=None, skills=False)
        self.assertIn("--strict-mcp-config", cmd)
        self.assertNotIn("--mcp-config", cmd)

    def test_the_answerer_arm_names_its_config(self):
        cmd = claude_cmd(mcp="/tmp/mcp.json", skills=False)
        self.assertIn("--strict-mcp-config", cmd)
        self.assertEqual(cmd[cmd.index("--mcp-config") + 1], "/tmp/mcp.json")

    def test_the_coverage_judge_restricts(self):
        import check_coverage as cc
        a = argparse.Namespace(agent_model="sonnet", set_dir=HERE,
                               timeout=1, retries=0, publisher=None)
        with mock.patch.object(cc, "run_cli",
                               return_value=([], "", "", 1, 0.0)) as run:
            cc.judge_case({"qid": "q", "question": "?"}, "source: s is x",
                          a, cc.verdicts())
        cmd = run.call_args.args[0]
        self.assertIn("--strict-mcp-config", cmd)
        self.assertNotIn("--mcp-config", cmd)


class TheFence(unittest.TestCase):
    """One definition of the deny-list. It was two -- 28 names for the
    answerer, 2 for everything spawned through `spawn_agent` -- so the agent
    under measurement was confined and the instrumentation scoring it was
    not."""

    def test_every_spawned_agent_is_denied_the_dangerous_tools(self):
        # Each of these was measured as GRANTED to the clustering agent on
        # 2026-09-08. `Task` spawns a sub-agent confined by nothing;
        # `ReportFindings` cost the coverage judge its only turn.
        for tool in ("Task", "ReportFindings", "SendMessage", "Workflow",
                     "ScheduleWakeup", "RemoteTrigger", "ShareOnboardingGuide",
                     "WebFetch", "WebSearch", "ReadMcpResourceTool"):
            with self.subTest(tool=tool):
                self.assertIn(tool, ah.ALWAYS_BLOCKED)
                self.assertIn(tool, spawn_cmd(mcp_url=None))

    def test_the_edit_tools_are_role_dependent_not_always_blocked(self):
        # skill:eval-improve must edit the model and run its reload script, and
        # --disallowedTools beats --allowedTools, so naming these globally
        # would make IMPROVE_TOOLS unusable rather than merely redundant.
        for tool in ("Edit", "Write", "NotebookEdit", "Bash"):
            with self.subTest(tool=tool):
                self.assertNotIn(tool, ah.ALWAYS_BLOCKED)

    def test_the_answerer_fence_is_the_core_plus_both_groups(self):
        self.assertEqual(set(rb.BLOCKED_TOOLS),
                         set(ah.ALWAYS_BLOCKED) | set(ah.NO_EDITS)
                         | set(ah.NO_SHELL))

    def test_the_answerer_fence_has_no_duplicate_names(self):
        self.assertEqual(len(rb.BLOCKED_TOOLS), len(set(rb.BLOCKED_TOOLS)))

    def test_a_caller_can_only_add_to_the_fence(self):
        cmd = spawn_cmd(mcp_url=None, blocked=("Glob",))
        denied = cmd[cmd.index("--disallowedTools") + 1:]
        self.assertIn("Glob", denied)
        for tool in ah.ALWAYS_BLOCKED:
            self.assertIn(tool, denied)


class ReadOnlyRoles(unittest.TestCase):
    def test_diagnose_and_cluster_block_the_shell_too(self):
        # Blocking Edit/Write while leaving Bash granted only looks like a
        # fence: `bash -c` writes whatever Edit cannot.
        sys.path.insert(0, str(SKILLS_ROOT / "eval-diagnose" / "scripts"))
        import diagnose
        self.assertIn("Bash", diagnose.READ_ONLY)
        for tool in ah.NO_EDITS:
            self.assertIn(tool, diagnose.READ_ONLY)


if __name__ == "__main__":
    unittest.main(verbosity=1)
