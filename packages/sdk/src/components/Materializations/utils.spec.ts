// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { MaterializationStatus, type Materialization } from "../../client";
import {
   formatDuration,
   formatRelativeTime,
   formatTimestamp,
   isActiveStatus,
   isTerminalStatus,
   parseMetadata,
   sourcesSummary,
   statusColor,
   statusLabel,
   triggerLabel,
} from "./utils";

/**
 * The materializations table is entirely these functions: every cell it draws
 * is one of them, and the polling loop that keeps it live is driven by the two
 * predicates. None of it was covered, in either direction — a status the
 * publisher adds later would fall through to "Unknown" and be polled forever,
 * and nothing would say so.
 */

const ALL = Object.values(MaterializationStatus);

describe("status predicates", () => {
   it("treats the two in-flight phases as active", () => {
      expect(isActiveStatus(MaterializationStatus.Pending)).toBe(true);
      expect(isActiveStatus(MaterializationStatus.ManifestRowsReady)).toBe(
         true,
      );
   });

   it("treats the three settled phases as terminal", () => {
      expect(isTerminalStatus(MaterializationStatus.ManifestFileReady)).toBe(
         true,
      );
      expect(isTerminalStatus(MaterializationStatus.Failed)).toBe(true);
      expect(isTerminalStatus(MaterializationStatus.Cancelled)).toBe(true);
   });

   /**
    * The one that matters for the polling loop: every status the client knows
    * is on exactly one side. A status that is neither is polled until the page
    * is closed, because the loop stops on terminal and nothing else.
    */
   it("puts every known status on exactly one side", () => {
      for (const status of ALL) {
         expect([isActiveStatus(status), isTerminalStatus(status)]).toContain(
            true,
         );
         expect(isActiveStatus(status) && isTerminalStatus(status)).toBe(false);
      }
   });

   it("says an absent status is neither, so a half-loaded row is not polled", () => {
      expect(isActiveStatus(undefined)).toBe(false);
      expect(isTerminalStatus(undefined)).toBe(false);
   });
});

describe("statusLabel", () => {
   it("collapses both in-flight phases to one word", () => {
      // The publisher drives the phases itself, so the distinction between
      // them is not one a reader can act on.
      expect(statusLabel(MaterializationStatus.Pending)).toBe("Pending");
      expect(statusLabel(MaterializationStatus.ManifestRowsReady)).toBe(
         "Pending",
      );
   });

   it("names the settled phases apart", () => {
      expect(statusLabel(MaterializationStatus.ManifestFileReady)).toBe("Done");
      expect(statusLabel(MaterializationStatus.Failed)).toBe("Failed");
      expect(statusLabel(MaterializationStatus.Cancelled)).toBe("Cancelled");
   });

   it("never renders a raw enum value at a reader", () => {
      for (const status of ALL) {
         expect(statusLabel(status)).not.toContain("_");
      }
      expect(statusLabel(undefined)).toBe("Unknown");
   });
});

describe("statusColor", () => {
   it("pairs failure with error and success with success", () => {
      expect(statusColor(MaterializationStatus.Failed)).toBe("error");
      expect(statusColor(MaterializationStatus.ManifestFileReady)).toBe(
         "success",
      );
      expect(statusColor(MaterializationStatus.Cancelled)).toBe("warning");
   });

   it("gives every known status a colour the theme defines", () => {
      const allowed = ["default", "info", "success", "error", "warning"];
      for (const status of ALL) expect(allowed).toContain(statusColor(status));
      expect(allowed).toContain(statusColor(undefined));
   });
});

describe("parseMetadata and triggerLabel", () => {
   const run = (metadata: unknown) =>
      ({ metadata }) as unknown as Materialization;

   it("reads the fields a run carries", () => {
      const meta = parseMetadata(
         run({ trigger: "SCHEDULER", sourcesBuilt: 3 }),
      );
      expect(meta.trigger).toBe("SCHEDULER");
      expect(meta.sourcesBuilt).toBe(3);
   });

   it("hands back an empty object when there is no metadata", () => {
      // The generated client types this as `object | null`, so every caller
      // reads through here rather than risking a null dereference per cell.
      expect(parseMetadata(run(null))).toEqual({});
      expect(parseMetadata(run(undefined))).toEqual({});
   });

   it("calls a run manual unless the scheduler says otherwise", () => {
      expect(triggerLabel({ trigger: "SCHEDULER" })).toBe("Scheduled");
      expect(triggerLabel({ trigger: "ON_DEMAND" })).toBe("Manual");
      expect(triggerLabel({})).toBe("Manual");
   });
});

describe("formatDuration", () => {
   const from = "2026-07-14T06:00:00.000Z";
   const plus = (ms: number) => new Date(Date.parse(from) + ms).toISOString();

   it("steps through the units as the run gets longer", () => {
      expect(formatDuration(from, plus(250))).toBe("250ms");
      expect(formatDuration(from, plus(4_000))).toBe("4s");
      expect(formatDuration(from, plus(90_000))).toBe("1m 30s");
      expect(formatDuration(from, plus(3_600_000 + 120_000))).toBe("1h 2m");
   });

   it("measures an open run against now rather than reporting nothing", () => {
      const started = new Date(Date.now() - 5_000).toISOString();
      expect(formatDuration(started, null)).toMatch(/^[45]s$/);
   });

   it("clamps a completion that precedes its start instead of going negative", () => {
      expect(formatDuration(plus(5_000), from)).toBe("0ms");
   });

   it("renders a dash for a run that never started", () => {
      expect(formatDuration(undefined, plus(1000))).toBe("-");
      expect(formatDuration(null, null)).toBe("-");
   });
});

describe("formatTimestamp", () => {
   it("renders an instant", () => {
      expect(formatTimestamp("2026-07-14T06:00:00Z")).toContain("2026");
   });

   it("renders an em dash rather than 'Invalid Date' for junk", () => {
      expect(formatTimestamp("not a date")).toBe("—");
      expect(formatTimestamp(null)).toBe("—");
      expect(formatTimestamp(undefined)).toBe("—");
   });
});

describe("formatRelativeTime", () => {
   const ago = (ms: number) => new Date(Date.now() - ms).toISOString();

   it("steps through the units as the instant recedes", () => {
      expect(formatRelativeTime(ago(2_000))).toBe("just now");
      expect(formatRelativeTime(ago(5 * 60_000))).toBe("5m ago");
      expect(formatRelativeTime(ago(2 * 3_600_000))).toBe("2h ago");
      expect(formatRelativeTime(ago(3 * 86_400_000))).toBe("3d ago");
   });

   it("renders a dash when there is no instant", () => {
      expect(formatRelativeTime(null)).toBe("-");
      expect(formatRelativeTime(undefined)).toBe("-");
   });
});

describe("sourcesSummary", () => {
   it("reads as built and reused on a run that refused nothing", () => {
      expect(sourcesSummary({ sourcesBuilt: 2, sourcesReused: 1 }, ", ")).toBe(
         "2 built, 1 reused",
      );
   });

   it("names the refused count when the run skipped a source", () => {
      expect(
         sourcesSummary(
            { sourcesBuilt: 5, sourcesReused: 0, sourcesRefused: 7 },
            " · ",
         ),
      ).toBe("5 built · 0 reused · 7 refused");
   });
});
