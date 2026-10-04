// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { isTextTile, type DashboardDocument } from "./document";
import { artifactTag } from "./malloyText";
import {
   spliceDashboardDocument,
   spliceFailed,
   syntaxErrors,
} from "./spliceDocument";
import { openDocument } from "./testing/fixtures";

const BLOCK = `##! experimental.givens
##| artifact { kind=notebook title="Review"
  tiles=[
    intro { kind=text },
    "a_tiles -> overview",
    "a_tiles -> detail"
  ]
}
|##
import { a } from "../m.malloy"

##|(markdown) intro
Welcome.
|##

source: a_tiles is a extend {
  view: overview is by_cat
  view: detail is by_day
}
`;

const SINGLE = BLOCK.replace(
   /##\| artifact[\s\S]*?\|##\n/,
   `## artifact { kind=notebook title="Review" tiles=[intro { kind=text }, "a_tiles -> overview", "a_tiles -> detail"] }\n`,
);

async function writes(
   source: string,
   edit: (document: DashboardDocument) => void,
   options?: { changeKind?: boolean },
): Promise<{ out: string; document: DashboardDocument }> {
   const next = structuredClone(await openDocument(source));
   edit(next);
   const result = await spliceDashboardDocument(source, next, options);
   if (spliceFailed(result)) throw new Error(result.reason);
   expect(await syntaxErrors(result.source)).toEqual([]);
   return { out: result.source, document: await openDocument(result.source) };
}

describe("the artifact tag written as a ##| block", () => {
   it("is found by line, with its closer outside its text", () => {
      const tag = artifactTag(BLOCK.split("\n"));
      expect(tag).toMatchObject({ from: 1, to: 8, block: true });
      expect(tag?.text.startsWith("##| artifact {")).toBe(true);
      expect(tag?.text).not.toContain("|##");
      expect(artifactTag(SINGLE.split("\n"))).toMatchObject({
         from: 1,
         to: 1,
         block: false,
      });
   });

   it("reads back to the same document as the one-line spelling", async () => {
      const block = await openDocument(BLOCK);
      const single = await openDocument(SINGLE);
      expect(block).toEqual(single);
      expect(block.kind).toBe("notebook");
      expect(block.title).toBe("Review");
      expect(block.tiles.map((t) => t.name)).toEqual([
         "intro",
         "overview",
         "detail",
      ]);
   });

   it("changes nothing for an edit that changes nothing", async () => {
      expect((await writes(BLOCK, () => {})).out).toBe(BLOCK);
   });

   it("reorders in place, one entry per line as it was written", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.tiles.reverse();
      });
      expect(out).toContain(
         `  tiles=[\n    "a_tiles -> detail",\n    "a_tiles -> overview",\n    intro { kind=text }\n  ]\n}\n|##\n`,
      );
      expect(document.tiles.map((t) => t.name)).toEqual([
         "detail",
         "overview",
         "intro",
      ]);
   });

   it("removes an entry and its view, keeping the other lines", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "detail");
      });
      expect(out).toContain(
         `  tiles=[\n    intro { kind=text },\n    "a_tiles -> overview"\n  ]\n}\n|##\n`,
      );
      expect(out).not.toContain("detail");
      expect(document.tiles.map((t) => t.name)).toEqual(["intro", "overview"]);
   });

   it("adds an entry on a line of its own, and a text tile's entry with it", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.tiles.push({ kind: "text", name: "outro", markdown: "Bye." });
         d.tiles.push({
            name: "extra",
            source: "a_tiles",
            declaration: { kind: "reference", from: "by_x" },
         });
      });
      expect(out).toContain(
         `    "a_tiles -> detail",\n    outro { kind=text },\n    "a_tiles -> extra"\n  ]\n}\n|##\n`,
      );
      expect(out).toContain("##|(markdown) outro\nBye.\n|##\n");
      expect(document.tiles.map((t) => t.name)).toEqual([
         "intro",
         "overview",
         "detail",
         "outro",
         "extra",
      ]);
   });

   it("keeps the block when a text tile's width changes", async () => {
      const { out } = await writes(BLOCK, (d) => {
         const intro = d.tiles.find(isTextTile);
         if (intro) intro.colspan = 6;
      });
      expect(out).toContain(`    intro { kind=text colspan=6 },\n`);
      expect(out).toContain(`##| artifact { kind=notebook title="Review"\n`);
   });

   it("patches the title where it is, without collapsing the lines", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.title = "Renamed";
      });
      expect(out).toContain(
         `##| artifact { kind=notebook title="Renamed"\n  tiles=[\n`,
      );
      expect(document.title).toBe("Renamed");
   });

   it("adds a property on its own line inside the block", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.autorun = false;
      });
      expect(out).toContain(
         `    "a_tiles -> detail"\n  ]\n  autorun=false\n}\n|##\n`,
      );
      expect(document.autorun).toBe(false);
   });

   it("toggles the kind both ways, leaving the layout alone", async () => {
      const asDashboard = await writes(
         BLOCK,
         (d) => {
            d.kind = undefined;
            d.columns = 12;
         },
         { changeKind: true },
      );
      expect(asDashboard.out).toContain(
         `##| artifact { title="Review"\n  tiles=[\n`,
      );
      expect(asDashboard.document.kind).toBeUndefined();
      expect(asDashboard.document.columns).toBe(12);
      expect(asDashboard.out).toContain("dashboard { columns=12 }");
      const back = await writes(
         asDashboard.out,
         (d) => {
            d.kind = "notebook";
            d.columns = undefined;
         },
         { changeKind: true },
      );
      expect(back.document.kind).toBe("notebook");
      expect(back.document.columns).toBeUndefined();
      expect(back.out).not.toContain("dashboard {");
      expect(back.out).toContain(`\n  tiles=[\n    intro { kind=text },\n`);
   });

   it("adds, changes and removes starting givens inside the block", async () => {
      const added = await writes(BLOCK, (d) => {
         d.startingGivens = { REGION: "f'US'" };
      });
      expect(added.out).toContain(`  ]\n  givens { REGION="f'US'" }\n}\n|##\n`);
      expect(added.document.startingGivens).toEqual({ REGION: "f'US'" });

      const changed = await writes(added.out, (d) => {
         d.startingGivens = { REGION: "f'US'", SINCE: "2023-01-01" };
      });
      expect(changed.out).toContain(
         `givens { REGION="f'US'" SINCE="2023-01-01" }\n}\n|##\n`,
      );
      expect(changed.out.match(/givens \{/g)).toHaveLength(1);
      expect(changed.document.startingGivens).toEqual({
         REGION: "f'US'",
         SINCE: "2023-01-01",
      });

      const removed = await writes(changed.out, (d) => {
         d.startingGivens = undefined;
      });
      expect(removed.out).toBe(BLOCK);
   });

   it("takes autorun out again, leaving the block as it was", async () => {
      const off = await writes(BLOCK, (d) => {
         d.autorun = false;
      });
      const { out, document } = await writes(off.out, (d) => {
         d.autorun = undefined;
      });
      expect(out).toBe(BLOCK);
      expect(document.autorun).toBeUndefined();
   });

   it("writes a description above the block and reads it back", async () => {
      const { out, document } = await writes(BLOCK, (d) => {
         d.description = "What this review covers.";
      });
      expect(document.description).toBe("What this review covers.");
      expect(out).toContain(`##" What this review covers.\n`);
      expect(out).toContain(
         `##| artifact { kind=notebook title="Review"\n  tiles=[\n`,
      );
      const cleared = await writes(out, (d) => {
         d.description = undefined;
      });
      expect(cleared.out).toBe(BLOCK);
   });

   it("leaves a one-line tag one line through the same edits", async () => {
      const { out } = await writes(SINGLE, (d) => {
         d.tiles.reverse();
         d.title = "Renamed";
      });
      expect(out.split("\n")[1]).toBe(
         `## artifact { kind=notebook title="Renamed" tiles=["a_tiles -> detail", "a_tiles -> overview", intro { kind=text }] }`,
      );
   });
});
