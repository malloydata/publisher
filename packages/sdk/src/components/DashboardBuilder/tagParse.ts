/** A MOTLY parse as `parseAnnotation` returns it; only the reads this file makes. */
type Parsed<T> = { tag: T; log: Array<{ message: string }> };
type ParseAnnotation<T> = (lines: string[]) => Parsed<T>;

/**
 * Index just past the delimited region starting at `start` (a quoted string,
 * quoted identifier or heredoc), or `start` when nothing is delimited there.
 * Unterminated regions run to the end so the caller never rewrites inside one.
 */
function endOfDelimited(text: string, start: number): number {
   const char = text[start];
   if (char === "`") {
      for (let i = start + 1; i < text.length; i++) {
         if (text[i] === "\\") i++;
         else if (text[i] === "`") return i + 1;
         else if (text[i] === "\n") return text.length;
      }
      return text.length;
   }
   if (text.startsWith("<<<", start)) {
      // Split on `\n` only and compare with `trim()`, as MOTLY does; a `/m` regex also breaks at `\r`.
      let lineStart = start + 3;
      for (;;) {
         const newline = text.indexOf("\n", lineStart);
         const lineEnd = newline === -1 ? text.length : newline;
         if (text.slice(lineStart, lineEnd).trim() === ">>>") return lineEnd;
         if (newline === -1) return text.length;
         lineStart = newline + 1;
      }
   }
   if (char === '"' || char === "'") {
      const triple = char.repeat(3);
      if (text.startsWith(triple, start)) {
         for (let i = start + 3; i < text.length; i++) {
            if (text[i] === "\\") i++;
            else if (text.startsWith(triple, i)) return i + 3;
         }
         return text.length;
      }
      for (let i = start + 1; i < text.length; i++) {
         if (text[i] === "\\") i++;
         else if (text[i] === char) return i + 1;
         else if (text[i] === "\n") return i;
      }
      return text.length;
   }
   return start;
}

/**
 * Quote each bare filter literal (`f'US'`) so MOTLY reads it as a string, as
 * the server does. The result is a read copy only; it is never written back.
 */
export function quoteFilterLiterals(annotation: string): string {
   const BARE_FILTER_LITERAL = /^([ \t]*)f(['"])((?:\\.|(?!\2)[^\\])*)\2/;
   let out = "";
   let i = 0;
   while (i < annotation.length) {
      const delimited = endOfDelimited(annotation, i);
      if (delimited > i) {
         out += annotation.slice(i, delimited);
         i = delimited;
         continue;
      }
      const char = annotation[i];
      if (/[=[,]/.test(char)) {
         const match = BARE_FILTER_LITERAL.exec(annotation.slice(i + 1));
         if (match) {
            const body = match[3].replace(/\\/g, "\\\\").replace(/"/g, '\\"');
            out += `${char}${match[1]}"f'${body}'"`;
            i += 1 + match[0].length;
            continue;
         }
      }
      out += char;
      i += 1;
   }
   return out;
}

/**
 * `parseAnnotation` that rescues bare filter literals. Parse-first: only a line
 * MOTLY rejects is rewritten, and only kept if the rewrite parses, so a valid
 * tag is never altered. `errors` are the parse messages that remain.
 */
export function parseTagLines<T>(
   parseAnnotation: ParseAnnotation<T>,
   lines: string[],
): { tag: T; errors: string[] } {
   const direct = parseAnnotation(lines);
   if (direct.log.length === 0) return { tag: direct.tag, errors: [] };
   const rescued = lines.map((line) => {
      if (parseAnnotation([line]).log.length === 0) return line;
      const rewritten = quoteFilterLiterals(line);
      return parseAnnotation([rewritten]).log.length === 0 ? rewritten : line;
   });
   const after = parseAnnotation(rescued);
   return { tag: after.tag, errors: after.log.map((e) => e.message) };
}
