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
      const back = await writes(
         asDashboard.out,
         (d) => {
            d.kind = "notebook";
            d.columns = undefined;
         },
         { changeKind: true },
      );
      expect(back.document.kind).toBe("notebook");
      expect(back.out).toContain(`\n  tiles=[\n    intro { kind=text },\n`);
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
