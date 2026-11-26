#!/usr/bin/env python3
"""Installing a package's own skills into an answerer workspace.

Two things can go wrong here and neither announces itself on a live run.

The first is contamination. An eval set lives at `<package>/evals/`, so a copy
that reached one directory too wide would put `cases.jsonl` inside the
answerer's cwd, where `Read` reaches it. The run would still complete and still
report a number.

The second is a mislabelled arm. `--package-skills=install` that installs
nothing is the `off` arm wearing the `install` label, and the comparison the
mode exists to support silently measures the same thing twice.

Stdlib only: python3 package_skills_test.py
"""
import json
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import argparse  # noqa: E402
from agent_harness import (build_workspace, package_skill_dirs,  # noqa: E402
                           package_skills_sha)
import run_baseline as rb  # noqa: E402


def write_skill(root: pathlib.Path, name: str, body: str = "body") -> None:
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / "SKILL.md").write_text(
        f"---\nname: {name}\ndescription: d\n---\n\n{body}\n")


class InstallPackageSkills(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="pkgskills-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        # The manifest side: skills the harness would install anyway.
        self.skills_root = self.tmp / "skills-root"
        write_skill(self.skills_root, "malloy-analysis", "BUNDLED ANALYSIS")
        write_skill(self.skills_root, "malloy-queries", "BUNDLED QUERIES")
        # The package side, with an eval set beside it.
        self.package = self.tmp / "pkg"
        (self.package).mkdir()
        (self.package / "publisher.json").write_text("{}")
        self.pkg_skills = (self.package / "workflows" / "default"
                           / "skill_sets" / "house")
        write_skill(self.pkg_skills, "house-conventions", "OURS")
        evals = self.package / "evals" / "set-a"
        evals.mkdir(parents=True)
        (evals / "cases.jsonl").write_text('{"qid":"q1","golden":42}\n')

    def build(self, **kw) -> pathlib.Path:
        work = build_workspace(["malloy-analysis", "malloy-queries"],
                               self.skills_root, mcp_url=None, **kw)
        self.addCleanup(shutil.rmtree, work, ignore_errors=True)
        return work

    def test_installs_the_package_skill_alongside_the_manifest_ones(self):
        work = self.build(package_skills_dirs=[self.pkg_skills])
        installed = sorted(p.name for p in (work / ".claude" / "skills").iterdir())
        self.assertEqual(
            installed, ["house-conventions", "malloy-analysis", "malloy-queries"])

    def test_package_skill_shadows_a_manifest_skill_of_the_same_name(self):
        # The server resolves a same-named package skill by replacing the
        # bundled one. The installed path has to agree, or the two arms of the
        # comparison disagree about which guide is in force.
        write_skill(self.pkg_skills, "malloy-analysis", "OURS WINS")
        work = self.build(package_skills_dirs=[self.pkg_skills])
        body = (work / ".claude" / "skills" / "malloy-analysis"
                / "SKILL.md").read_text()
        self.assertIn("OURS WINS", body)
        self.assertNotIn("BUNDLED ANALYSIS", body)

    def test_never_copies_the_eval_set(self):
        work = self.build(package_skills_dirs=[self.pkg_skills])
        found = [str(p.relative_to(work)) for p in work.rglob("*")
                 if p.name in ("cases.jsonl", "set.json", "events.jsonl")
                 or p.name == "evals"]
        self.assertEqual(found, [], f"eval artefacts reached the workspace: {found}")

    def test_does_not_follow_a_link_inside_a_skill_into_the_eval_set(self):
        # skills/x/evals -> ../../evals would otherwise copy the goldens.
        link = self.pkg_skills / "house-conventions" / "evals"
        link.symlink_to(self.package / "evals", target_is_directory=True)
        work = self.build(package_skills_dirs=[self.pkg_skills])
        found = [str(p.relative_to(work)) for p in work.rglob("*")
                 if p.name in ("cases.jsonl", "set.json", "evals")]
        self.assertEqual(found, [], f"a link carried the eval set in: {found}")

    def test_a_skill_set_may_have_any_name(self):
        # A set is named by its folder, which is not necessarily `skills`.
        work = self.build(package_skills_dirs=[self.pkg_skills])
        self.assertTrue((work / ".claude" / "skills" / "house-conventions"
                         / "SKILL.md").is_file())

    def test_refuses_a_package_root(self):
        # The guard is on "is this a package root" rather than on its contents,
        # because "does this tree contain goldens" is a question that can be
        # answered wrongly and "is this a package" cannot.
        with self.assertRaises(ValueError):
            self.build(package_skills_dirs=[self.package])

    def test_takes_only_folders_that_hold_a_skill(self):
        (self.pkg_skills / "evals").mkdir()
        (self.pkg_skills / "evals" / "cases.jsonl").write_text("{}\n")
        self.assertEqual(sorted(package_skill_dirs([self.pkg_skills])),
                         ["house-conventions"])

    def test_the_first_listed_set_wins_a_name_clash(self):
        other = self.tmp / "other-set"
        write_skill(other, "house-conventions", "THEIRS")
        write_skill(other, "extra", "EXTRA")
        work = self.build(package_skills_dirs=[self.pkg_skills, other])
        skills = work / ".claude" / "skills"
        self.assertIn("OURS", (skills / "house-conventions"
                               / "SKILL.md").read_text())
        self.assertTrue((skills / "extra" / "SKILL.md").is_file())

    def test_the_sha_is_of_what_is_installed(self):
        before = package_skills_sha(package_skill_dirs([self.pkg_skills]))
        self.assertRegex(before, r"^[0-9a-f]{64}$")
        write_skill(self.pkg_skills, "house-conventions", "EDITED")
        self.assertNotEqual(
            before, package_skills_sha(package_skill_dirs([self.pkg_skills])))

    def test_the_sha_ignores_a_linked_file_that_is_not_installed(self):
        before = package_skills_sha(package_skill_dirs([self.pkg_skills]))
        (self.pkg_skills / "house-conventions" / "notes.md").symlink_to(
            self.package / "publisher.json")
        self.assertEqual(
            before, package_skills_sha(package_skill_dirs([self.pkg_skills])))

    def test_the_sha_ignores_a_link_that_is_not_installed(self):
        before = package_skills_sha(package_skill_dirs([self.pkg_skills]))
        (self.pkg_skills / "house-conventions" / "evals").symlink_to(
            self.package / "evals", target_is_directory=True)
        self.assertEqual(
            before, package_skills_sha(package_skill_dirs([self.pkg_skills])))

    def test_refuses_a_set_link_that_points_at_the_eval_set(self):
        shutil.rmtree(self.pkg_skills)
        self.pkg_skills.symlink_to(self.package / "evals", target_is_directory=True)
        with self.assertRaises(ValueError):
            self.build(package_skills_dirs=[self.pkg_skills])

    def test_refuses_a_set_link_even_when_it_resolves_to_a_real_set(self):
        real = self.tmp / "elsewhere" / "skill-set"
        write_skill(real, "x")
        shutil.rmtree(self.pkg_skills)
        self.pkg_skills.symlink_to(real, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.build(package_skills_dirs=[self.pkg_skills])

    def test_refuses_a_missing_directory_rather_than_installing_nothing(self):
        with self.assertRaises(FileNotFoundError):
            self.build(package_skills_dirs=[self.package / "skills-typo"])

    def test_installs_nothing_when_not_asked(self):
        work = self.build()
        installed = sorted(p.name for p in (work / ".claude" / "skills").iterdir())
        self.assertEqual(installed, ["malloy-analysis", "malloy-queries"])

    def test_copies_reference_files_beside_the_skill(self):
        # Symlinking put reference/*.md outside the session's allowed
        # directory, so the agent could load SKILL.md and read nothing beside
        # it. Package skills must not reintroduce that.
        ref = self.pkg_skills / "house-conventions" / "reference"
        ref.mkdir(parents=True)
        (ref / "margin.md").write_text("# Margin\n")
        work = self.build(package_skills_dirs=[self.pkg_skills])
        target = (work / ".claude" / "skills" / "house-conventions"
                  / "reference" / "margin.md")
        self.assertTrue(target.is_file())
        self.assertFalse(target.is_symlink())


class ResolveArm(unittest.TestCase):
    """`--package-skills` must describe the run that actually happened.

    Each mode is a claim about three things that live apart: the flag, the
    files, and whether the deployment serves them at all. When they disagree the
    run still completes and still reports a number, under the wrong label, and
    nothing downstream can tell. So a mismatch has to stop the run.
    """

    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="arm-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        # The package's default workflow lists one skill set, as a migrated
        # package does.
        self.pkg = self.tmp / "pkg"
        self.pkg_skills = self.pkg / "workflows" / "default" / "skill_sets" / "house"
        write_skill(self.pkg_skills, "house", "OURS")
        self.manifest(["workflows/default/skill_sets/house"])
        # Stub the HTTP layer: nothing here may reach a network.
        self.served_dir = self.pkg
        for name, fake in (
                ("package_skill_names", lambda *a, **k: self.served_names),
                ("served_package_dir", lambda *a, **k: self.served_dir)):
            real = getattr(rb, name)
            setattr(rb, name, fake)
            self.addCleanup(setattr, rb, name, real)
        self.served_names: list[str] = []

    def serve(self, names: list[str]) -> None:
        self.served_names = names

    def manifest(self, sets: list[str]) -> None:
        (self.pkg / "publisher.json").write_text(json.dumps(
            {"name": "pkg",
             "workflows": {"default": {"skill_sets": sets}}}))

    def args(self, mode: str, override=None) -> argparse.Namespace:
        return argparse.Namespace(
            package_skills=mode, target="local", publisher="http://x",
            no_answerer_skills=False,
            environment="env", package="pkg",
            package_skills_dir_override=[str(override)] if override else None)

    def test_install_with_a_tree_resolves_and_pins_a_sha(self):
        self.serve(["house"])
        a = self.args("install", self.pkg_skills)
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills_dirs, [self.pkg_skills])
        self.assertRegex(a.package_skills_sha, r"^[0-9a-f]{64}$")

    def test_install_reads_the_default_workflows_sets_from_the_manifest(self):
        # No --package-skills-dir: the served copy's publisher.json decides,
        # and there is no folder called `skills` anywhere in it.
        self.serve(["house"])
        a = self.args("install")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills_dirs, [self.pkg_skills])
        self.assertEqual(a.package_skill_names, ["house"])

    def test_install_reads_every_set_in_the_order_listed(self):
        write_skill(self.pkg / "workflows" / "weekly" / "skill_sets" / "extra",
                    "extra")
        self.manifest(["workflows/default/skill_sets/house",
                       "workflows/weekly/skill_sets/extra"])
        self.serve(["extra", "house"])
        a = self.args("install")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skill_names, ["extra", "house"])
        self.assertEqual([p.name for p in a.package_skills_dirs],
                         ["house", "extra"])

    def test_a_root_skills_dir_listed_as_a_set_is_read(self):
        write_skill(self.pkg / "skills", "legacy")
        self.manifest(["skills"])
        self.serve(["legacy"])
        a = self.args("install")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skill_names, ["legacy"])

    def test_a_default_the_server_refuses_installs_and_pins_nothing(self):
        # The server refuses the whole workflow for one missing set, so it
        # serves no package skills; the valid set beside it must not leak
        # into the install arm or the pin.
        self.manifest(["workflows/default/skill_sets/house", "nope"])
        self.serve([])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("install"))
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills, "off")
        self.assertIsNone(a.package_skills_sha)

    def test_install_refuses_when_the_server_serves_other_skills(self):
        self.serve(["house", "someone-elses"])
        with self.assertRaises(SystemExit) as raised:
            rb.resolve_package_skills(self.args("install"))
        self.assertIn("someone-elses", str(raised.exception))

    def test_install_refuses_a_set_that_holds_no_skills(self):
        empty = self.pkg / "workflows" / "default" / "skill_sets" / "empty"
        empty.mkdir()
        self.manifest(["workflows/default/skill_sets/empty"])
        self.serve([])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("install"))

    def test_the_tool_arm_pins_the_sha_of_the_default_sets(self):
        # The pin that tells two runs apart by guidance must not go null for
        # a package laid out as workflows.
        self.serve(["house"])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertRegex(a.package_skills_sha, r"^[0-9a-f]{64}$")
        self.assertEqual(a.package_skills_dirs, [])

    def test_the_tool_arm_pins_nothing_when_the_served_files_are_not_local(self):
        # The server serves skills but their tree is not on this machine: a
        # hash of nothing would pin the run as "guidance: none".
        self.served_dir = None
        self.serve(["house"])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills, "tool")
        self.assertIsNone(a.package_skills_sha)

    def test_the_tool_arm_pins_nothing_when_local_skills_differ_from_served(self):
        # Hashing local skills the answerer cannot fetch would pin guidance
        # it never had.
        self.serve(["not-local"])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertIsNone(a.package_skills_sha)

    def test_a_package_with_no_default_workflow_has_nothing_to_install(self):
        (self.pkg / "publisher.json").write_text('{"name": "pkg"}')
        self.serve([])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("install"))
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertIsNone(a.package_skills_sha)

    def test_the_sha_moves_when_a_guide_changes(self):
        self.serve(["house"])
        a = self.args("install", self.pkg_skills)
        rb.resolve_package_skills(a)
        before = a.package_skills_sha
        write_skill(self.pkg_skills, "house", "EDITED")
        b = self.args("install", self.pkg_skills)
        rb.resolve_package_skills(b)
        self.assertNotEqual(before, b.package_skills_sha)

    def test_install_without_a_tree_refuses(self):
        self.serve(["house"])
        a = self.args("install", self.tmp / "nope" / "skill-set")
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(a)

    def test_tool_with_no_package_skills_is_recorded_as_off(self):
        # get_skill is denied under `off`, so that is what the run measures.
        self.serve([])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills, "off")
        self.assertNotIn(rb.TOOL_ARM_TOOLS[0], rb.answer_tools(a))
        self.assertIn(rb.TOOL_ARM_TOOLS[0], rb.answer_denied(a))

    def test_install_with_no_answerer_skills_is_fatal(self):
        self.serve(["house"])
        a = self.args("install", self.pkg_skills)
        a.no_answerer_skills = True
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(a)

    def test_a_replay_carries_the_prior_run_and_resolves_nothing(self):
        # Prior run was install; the server and files have since changed.
        self.serve([])
        a = self.args("tool")
        rb.resolve_package_skills(a, {"packageSkillsMode": "install",
                                      "packageSkillsSha": "abc"})
        self.assertEqual((a.package_skills, a.package_skills_sha),
                         ("install", "abc"))
        self.assertEqual(a.package_skills_dirs, [])

    def test_a_replay_of_a_run_that_recorded_nothing_records_nothing(self):
        a = self.args("tool")
        rb.resolve_package_skills(a, {})
        self.assertIsNone(a.package_skills)
        self.assertIsNone(a.package_skills_sha)

    def test_a_platform_run_records_null_without_probing(self):
        def boom(*a, **k):
            raise AssertionError("probed the REST API")
        rb.package_skill_names = boom
        a = self.args("tool")
        a.target = "platform"
        rb.resolve_package_skills(a)
        self.assertIsNone(a.package_skills)

    def test_refuses_a_served_skill_set_that_is_a_link(self):
        pkg = self.tmp / "served"
        (pkg / "evals").mkdir(parents=True)
        (pkg / "house").symlink_to(pkg / "evals", target_is_directory=True)
        (pkg / "publisher.json").write_text(json.dumps(
            {"workflows": {"default": {"skill_sets": ["house"]}}}))
        self.served_dir = pkg
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("tool"))

    def test_tool_passes_when_the_server_serves_them(self):
        self.serve(["house"])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills_dirs, [])

    def test_off_refuses_when_the_server_is_still_serving_them(self):
        # A baseline the answerer can call get_skill against is not a baseline.
        self.serve(["house"])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("off"))

    def test_off_passes_when_the_server_withholds_them(self):
        self.serve([])
        a = self.args("off")
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills_dirs, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
