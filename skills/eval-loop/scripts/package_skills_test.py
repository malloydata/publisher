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
import pathlib
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent
                       / "eval-answer" / "scripts"))
import argparse  # noqa: E402
from agent_harness import build_workspace  # noqa: E402
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
        self.pkg_skills = self.package / "skills"
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
        work = self.build(package_skills_dir=self.pkg_skills)
        installed = sorted(p.name for p in (work / ".claude" / "skills").iterdir())
        self.assertEqual(
            installed, ["house-conventions", "malloy-analysis", "malloy-queries"])

    def test_package_skill_shadows_a_manifest_skill_of_the_same_name(self):
        # The server resolves a same-named package skill by replacing the
        # bundled one. The installed path has to agree, or the two arms of the
        # comparison disagree about which guide is in force.
        write_skill(self.pkg_skills, "malloy-analysis", "OURS WINS")
        work = self.build(package_skills_dir=self.pkg_skills)
        body = (work / ".claude" / "skills" / "malloy-analysis"
                / "SKILL.md").read_text()
        self.assertIn("OURS WINS", body)
        self.assertNotIn("BUNDLED ANALYSIS", body)

    def test_never_copies_the_eval_set(self):
        work = self.build(package_skills_dir=self.pkg_skills)
        found = [str(p.relative_to(work)) for p in work.rglob("*")
                 if p.name in ("cases.jsonl", "set.json", "events.jsonl")
                 or p.name == "evals"]
        self.assertEqual(found, [], f"eval artefacts reached the workspace: {found}")

    def test_refuses_a_directory_that_is_not_named_skills(self):
        # The guard is on the directory name rather than on its contents,
        # because "does this tree contain goldens" is a question that can be
        # answered wrongly and "is this the skills directory" cannot.
        with self.assertRaises(ValueError):
            self.build(package_skills_dir=self.package)

    def test_refuses_a_missing_directory_rather_than_installing_nothing(self):
        with self.assertRaises(FileNotFoundError):
            self.build(package_skills_dir=self.package / "skills-typo")

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
        work = self.build(package_skills_dir=self.pkg_skills)
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
        self.pkg_skills = self.tmp / "pkg" / "skills"
        write_skill(self.pkg_skills, "house", "OURS")
        self._real = rb.package_skill_names
        self.addCleanup(setattr, rb, "package_skill_names", self._real)

    def serve(self, names: list[str]) -> None:
        rb.package_skill_names = lambda *a, **k: names

    def args(self, mode: str, override=None) -> argparse.Namespace:
        return argparse.Namespace(
            package_skills=mode, target="local", publisher="http://x",
            environment="env", package="pkg",
            package_skills_dir_override=str(override) if override else None)

    def test_install_with_a_tree_resolves_and_pins_a_sha(self):
        self.serve(["house"])
        a = self.args("install", self.pkg_skills)
        rb.resolve_package_skills(a)
        self.assertEqual(a.package_skills_dir, self.pkg_skills)
        self.assertRegex(a.package_skills_sha, r"^[0-9a-f]{64}$")

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
        a = self.args("install", self.tmp / "nope" / "skills")
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(a)

    def test_tool_refuses_when_the_server_serves_none(self):
        # Otherwise the answerer has nothing to fetch and this is the off arm.
        self.serve([])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("tool"))

    def test_tool_passes_when_the_server_serves_them(self):
        self.serve(["house"])
        a = self.args("tool")
        rb.resolve_package_skills(a)
        self.assertIsNone(a.package_skills_dir)

    def test_off_refuses_when_the_server_is_still_serving_them(self):
        # A baseline the answerer can call get_skill against is not a baseline.
        self.serve(["house"])
        with self.assertRaises(SystemExit):
            rb.resolve_package_skills(self.args("off"))

    def test_off_passes_when_the_server_withholds_them(self):
        self.serve([])
        a = self.args("off")
        rb.resolve_package_skills(a)
        self.assertIsNone(a.package_skills_dir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
