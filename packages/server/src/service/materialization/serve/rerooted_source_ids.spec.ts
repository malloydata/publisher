import { describe, expect, it } from "bun:test";
import { rerootedSourceIDsOf } from "./served_copy_serve";

describe("rerootedSourceIDsOf: the sources a compiled copy reads from a table", () => {
   const reroot = (name: string) => ({
      reroot: { sourceID: `${name}@m`, sourceName: name },
   });

   it("names each re-root, and each extension whose base is still re-rooted", () => {
      const ids = rerootedSourceIDsOf(
         [reroot("p"), reroot("k")],
         [
            { sourceID: "c@m", sourceName: "c", chain: ["p@m"] },
            { sourceID: "d@m", sourceName: "d", chain: [], baseName: "k" },
         ],
      );
      expect([...ids].sort()).toEqual(["c@m", "d@m", "k@m", "p@m"]);
   });

   it("leaves out an extension whose base a pass dropped", () => {
      const ids = rerootedSourceIDsOf(
         [reroot("k")],
         [
            { sourceID: "c@m", sourceName: "c", chain: ["p@m"] },
            { sourceID: "d@m", sourceName: "d", chain: [], baseName: "p" },
         ],
      );
      expect([...ids]).toEqual(["k@m"]);
   });
});
