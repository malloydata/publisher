// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { SimpleGitProgressEvent } from "simple-git";

/**
 * Options for every package git clone. Shallow, because only the default
 * branch's working tree is served and a reload re-clones, so history is pure
 * download cost.
 */
export const GIT_CLONE_OPTIONS: Record<string, string | number | null> = {
   "--depth": 1,
   "--single-branch": null,
};

/**
 * Drop git progress lines from a failed clone's error message (simple-git
 * forces `--progress` when a progress handler is set), since the message
 * reaches API error responses and logs. Returns the input rather than an
 * empty error when every line matched.
 */
export function stripGitProgressNoise(message: string): string {
   const kept = message
      .split(/\r\n|\r|\n/)
      .filter(
         (line) =>
            !/\d+% \(\d+\/\d+\)/.test(line) &&
            // Also matches the "Error: Cloning into ..." shape V8 seeds
            // into err.stack from the message's first line.
            !/^(\w+: )?Cloning into /.test(line.trim()),
      )
      .join("\n")
      .trim();
   return kept.length > 0 ? kept : message.trim();
}

/** e.g. `[examples] cloning malloydata/publisher (storefront, +2 more)`. */
export function cloneProgressLabel(
   repo: string,
   context?: { environmentName?: string; packageNames?: string[] },
): string {
   const env = context?.environmentName ? `[${context.environmentName}] ` : "";
   const names = context?.packageNames ?? [];
   const shown = names.length > 4 ? names.slice(0, 3) : names;
   const more = names.length - shown.length;
   const pkgs =
      shown.length > 0
         ? ` (${shown.join(", ")}${more > 0 ? `, +${more} more` : ""})`
         : "";
   return `${env}cloning ${repo}${pkgs}`;
}

/**
 * Streams git clone progress to stderr, not winston (whose level can hide it)
 * or stdout (which an MCP stdio transport would own). A TTY gets one line
 * rewritten in place; anything else gets a line per stage and per 25-point
 * step.
 */
export class CloneProgressReporter {
   private lastStage: string | undefined;
   private lastMilestone = -1;
   private wroteInPlace = false;
   private finished = false;
   /**
    * One reporter at a time owns a TTY's in-place line: environments load
    * concurrently at boot, and two reporters `\r`-rewriting the same line
    * would overwrite each other. Later reporters fall back to milestone
    * lines until the owner finishes. Keyed by stream so tests with fake
    * streams cannot collide.
    */
   private static ttyOwners = new WeakMap<object, CloneProgressReporter>();

   constructor(
      private readonly label: string,
      private readonly out: NodeJS.WriteStream = process.stderr,
   ) {}

   onProgress(event: SimpleGitProgressEvent): void {
      if (this.finished) {
         // A late event after done() must not re-claim the TTY line the
         // finishing newline just released.
         return;
      }
      const { stage, progress, processed, total } = event;
      const counts = total > 0 ? ` (${processed}/${total})` : "";
      const text = `${this.label}: ${stage} ${progress}%${counts}`;
      if (this.out.isTTY === true && this.claimTtyLine()) {
         // Clamped to the terminal width, since a wrapped line breaks the \r
         // rewrite; the label is shortened first to keep the percentage
         // visible. \x1b[K clears the tail of a longer previous line.
         const budget = Math.max(1, (this.out.columns || 80) - 1);
         const suffix = `: ${stage} ${progress}%${counts}`;
         let line: string;
         if (text.length <= budget) {
            line = text;
         } else if (suffix.length + 4 <= budget) {
            const labelBudget = budget - suffix.length;
            line = `${this.label.slice(0, labelBudget - 3)}...${suffix}`;
         } else {
            line = text.slice(0, budget);
         }
         this.out.write(`\r${line}\x1b[K`);
         this.wroteInPlace = true;
         return;
      }
      const milestone = Math.floor(progress / 25);
      if (stage !== this.lastStage || milestone < this.lastMilestone) {
         // A new stage, or a percentage restart inside one reported stage:
         // git's server-side counting and compressing phases both arrive as
         // stage "remote:", so a drop in progress marks the next sub-phase.
         this.lastStage = stage;
         this.lastMilestone = milestone;
      } else if (milestone > this.lastMilestone) {
         this.lastMilestone = milestone;
      } else {
         return;
      }
      if (this.out.isTTY === true) {
         // A non-owner on a TTY: the cursor may sit mid-line on the owner's
         // in-place output, so take over the row cleanly and scroll it.
         this.out.write(`\r${text}\x1b[K\n`);
         return;
      }
      this.out.write(`${text}\n`);
   }

   done(): void {
      this.finished = true;
      if (this.wroteInPlace) {
         this.out.write("\n");
      }
      if (CloneProgressReporter.ttyOwners.get(this.out) === this) {
         CloneProgressReporter.ttyOwners.delete(this.out);
      }
   }

   private claimTtyLine(): boolean {
      const owner = CloneProgressReporter.ttyOwners.get(this.out);
      if (owner === undefined) {
         CloneProgressReporter.ttyOwners.set(this.out, this);
         return true;
      }
      return owner === this;
   }
}
