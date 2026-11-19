// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { fieldNotes, noteSource } from "./field_notes";

describe("noteSource", () => {
   it("writes a single-line note on its own line", () => {
      expect(noteSource('#(doc) "a \\"b\\" |# c"\n', "  ")).toBe(
         '  #(doc) "a \\"b\\" |# c"\n',
      );
   });

   it("closes a multi-line note at the opener's column, its body deeper", () => {
      // As the compiler holds it: no closer, body dedented.
      const held = '#|\n# label="Week"\n\n  # format="0"';
      expect(noteSource(held, "    ")).toBe(
         '    #|\n      # label="Week"\n\n        # format="0"\n    |#\n',
      );
   });

   it("keeps a body line that looks like a closer inside the note", () => {
      // `|#` at column 0 of the held body: written deeper than the opener,
      // it is not at the opener's column, so it does not close the note.
      expect(noteSource("#|\n|# not a closer", "")).toBe(
         "#|\n  |# not a closer\n|#\n",
      );
   });
});

describe("fieldNotes", () => {
   it("reads inherited, block and own notes, outermost first, once each", () => {
      const note = (text: string) => ({ text });
      expect(
         fieldNotes({
            annotation: {
               inherits: { notes: [note("# a"), note("# b")] },
               blockNotes: [note("# b"), note("# c")],
               notes: [note("# d")],
            },
         }),
      ).toEqual(["# a", "# b", "# c", "# d"]);
   });
});
