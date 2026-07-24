import * as fs from "node:fs";
import * as path from "node:path";
import { skillsDir } from "@malloy-publisher/skills";
import { ScaffoldError } from "./errors";

export interface SkillInstall {
   /** How many skills were copied in. */
   installed: number;
   /** Skill names left alone because the target is a symlink. */
   skipped: string[];
   /** Skill names whose existing files the bundled copies replaced. */
   refreshed: string[];
   /**
    * Paths that were inside a skill directory, are not in the bundled copy of
    * that skill, and so were removed and not written back. Relative to the
    * skills directory (`malloy-review/NOTES.md`), directories with a trailing
    * separator.
    *
    * Reported separately from `refreshed` because they are a different loss.
    * "Refreshed" is read as "my edits to the skill files were reverted", which
    * is recoverable knowledge; a file the user put in the directory themselves
    * is simply gone, and nothing named it.
    */
   removed: string[];
   /** Why nothing at all was installed, when nothing was. */
   refused?: string;
}

/**
 * Fail early when the skills package shipped nothing. Installing zero skills used
 * to print `✓ Installed 0 Malloy skills` and exit 0, which reads as success for a
 * workspace whose whole point is that the skills are there.
 */
export function assertSkillsAvailable(): void {
   if (countSkills(skillsDir) === 0) {
      throw new ScaffoldError(
         `No Malloy skills found in ${skillsDir}. The @malloy-publisher/skills ` +
            `install looks incomplete; reinstall create-malloy-package and run again.`,
      );
   }
}

/**
 * Copy every agent skill into targetDir as real files. The generated project is
 * standalone and the user may move or commit it, so this is a recursive copy, not
 * a symlink: a symlink into node_modules would dangle the moment the project is
 * moved or its dependencies pruned. skillsDir already excludes non-shipping skills
 * and the internal README, so a straight copy is clean.
 *
 * A skill directory in the target that is itself a symlink is left alone. That is
 * how this repo's own contributors wire skills into their agent (skills/README.md
 * tells them to), and copying through the link would overwrite the source of truth
 * rather than the workspace.
 *
 * targetDir has to resolve inside workspaceRoot. A `.claude` that is itself a
 * symlink elsewhere is not caught by the per-skill check above, and used to write
 * every skill outside the workspace while reporting them installed.
 *
 * An existing skill directory is removed before the copy rather than copied over.
 * fs.cpSync follows a symlink it finds at the destination, so checking only the
 * skill directory itself left every path inside it writable through a link: a
 * `.claude/skills/malloy-review/SKILL.md` linked elsewhere had the file it points
 * at replaced, and a linked `reference/` (four shipped skills have one) had files
 * written into whatever directory it named, all under a green
 * "Installed N Malloy skills". `.claude/skills/` is shared agent territory where
 * links are an expected state, so the check has to cover the whole subtree, and
 * removing first is the only form of that which cannot be one level short.
 *
 * The copy is unconditional, so an edited skill is replaced on every run. That is
 * deliberate (an existing workspace picks up skill updates), but it is reported:
 * `refreshed` names what was overwritten. Because the directory is removed and
 * not merged, that goes further than reverting an edit: a file the user added
 * inside a skill directory is gone, and no bundled copy puts it back. Those paths
 * are collected into `removed` so the caller can name them, rather than leaving
 * them under a word ("refreshed") that reads as "your edits were reverted". Note
 * the copy never prunes whole skills, so one dropped from a later release stays in
 * the workspace until deleted by hand.
 */
export function installSkills(
   targetDir: string,
   workspaceRoot: string,
): SkillInstall {
   const empty = { installed: 0, skipped: [], refreshed: [], removed: [] };
   if (isSymlink(targetDir)) {
      return {
         ...empty,
         refused:
            "it is a symlink, so writing into it would edit whatever it points at",
      };
   }
   if (!isWithinDirectory(targetDir, workspaceRoot)) {
      return {
         ...empty,
         refused:
            "it resolves outside the workspace, so writing into it would put " +
            "files somewhere else on this machine",
      };
   }
   fs.mkdirSync(targetDir, { recursive: true });

   const skipped: string[] = [];
   const refreshed: string[] = [];
   const removed: string[] = [];
   let installed = 0;
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory()) {
         continue;
      }
      const source = path.join(skillsDir, entry.name);
      if (!fs.existsSync(path.join(source, "SKILL.md"))) {
         continue;
      }
      const target = path.join(targetDir, entry.name);
      if (isSymlink(target)) {
         skipped.push(entry.name);
         continue;
      }
      if (fs.existsSync(target)) {
         refreshed.push(entry.name);
         // Read before the removal, because after it there is nothing to read:
         // these are the paths the copy below does not put back.
         for (const relative of pathsNotIn(target, source)) {
            removed.push(path.join(entry.name, relative));
         }
      }
      // target is not a symlink (checked above) and sits directly under a
      // targetDir that resolves inside the workspace, so this removes files in
      // the workspace and, for any symlink further down, the link itself rather
      // than what it points at.
      fs.rmSync(target, { recursive: true, force: true });
      fs.cpSync(source, target, { recursive: true });
      installed += 1;
   }
   return { installed, skipped, refreshed, removed };
}

/**
 * Every path under target that source does not also hold, relative to target.
 *
 * A directory that source does not hold is named once and not walked: what the
 * user has to look for is the directory, and walking it would turn one dropped
 * folder into a hundred lines. Nothing here follows a link out of target either,
 * because a symlink is compared by its own path, never by what it points at.
 */
function pathsNotIn(target: string, source: string, prefix = ""): string[] {
   let entries: fs.Dirent[];
   try {
      entries = fs.readdirSync(path.join(target, prefix), {
         withFileTypes: true,
      });
   } catch {
      // Unreadable: nothing can be said about what is in it.
      return [];
   }
   const missing: string[] = [];
   for (const entry of entries) {
      const relative = path.join(prefix, entry.name);
      if (!fs.existsSync(path.join(source, relative))) {
         missing.push(entry.isDirectory() ? relative + path.sep : relative);
         continue;
      }
      if (entry.isDirectory()) {
         missing.push(...pathsNotIn(target, source, relative));
      }
   }
   return missing;
}

/** Count directories under dir that contain a SKILL.md, i.e. real skills. */
export function countSkills(dir: string): number {
   if (!fs.existsSync(dir)) {
      return 0;
   }
   return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter(
         (entry) =>
            entry.isDirectory() &&
            fs.existsSync(path.join(dir, entry.name, "SKILL.md")),
      ).length;
}

/**
 * Whether target, which need not exist yet, names a path inside root once every
 * symlink on the way has been followed. A path utility rather than a skills
 * concept, but it lives here because scaffold.ts already imports this module and
 * the reverse import would be a cycle.
 */
export function isWithinDirectory(target: string, root: string): boolean {
   const realRoot = realPathOf(root);
   const realTarget = realPathOf(target);
   return realTarget === realRoot || realTarget.startsWith(realRoot + path.sep);
}

/**
 * The path target really names. fs.realpathSync cannot be used: it throws on a
 * path that does not exist yet (every file we are about to create) and on a
 * dangling symlink, which is precisely the case that matters, because
 * fs.writeFileSync follows a dangling link and creates the file wherever it
 * points. So resolve the parent first, then follow one link at a time.
 */
function realPathOf(target: string, hops = 0): string {
   const resolved = path.resolve(target);
   const parent = path.dirname(resolved);
   if (parent === resolved) {
      return resolved;
   }
   const here = path.join(realPathOf(parent, hops), path.basename(resolved));
   if (hops >= 40) {
      // A symlink cycle. Stop following and judge the path as it stands.
      return here;
   }
   try {
      if (!fs.lstatSync(here).isSymbolicLink()) {
         return here;
      }
      return realPathOf(
         path.resolve(path.dirname(here), fs.readlinkSync(here)),
         hops + 1,
      );
   } catch {
      // Does not exist yet, or is unreadable: it is the path it looks like.
      return here;
   }
}

function isSymlink(target: string): boolean {
   try {
      return fs.lstatSync(target).isSymbolicLink();
   } catch {
      return false;
   }
}
