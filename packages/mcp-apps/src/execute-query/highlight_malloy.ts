// Lightweight, dependency-free Malloy syntax highlighter for the execute-query
// MCP app. Shiki is deliberately avoided: its Oniguruma wasm would be a large
// bump on a bundle that is inlined into the widget HTML, for a small query
// preview. This is a regex tokenizer approximating a github-light palette. It
// will not match a real grammar token-for-token, but it needs no wasm and no
// grammar dependency.
//
// Returns HTML: everything is HTML-escaped, recognized tokens wrapped in
// <span class="mh-*"> (styled in execute-query.html). Set via innerHTML on a
// pre-formatted element.

const KEYWORDS = new Set([
   "run",
   "source",
   "query",
   "dimension",
   "measure",
   "view",
   "join_one",
   "join_many",
   "join_cross",
   "is",
   "with",
   "on",
   "extend",
   "include",
   "where",
   "having",
   "group_by",
   "aggregate",
   "select",
   "calculate",
   "nest",
   "order_by",
   "limit",
   "top",
   "sample",
   "index",
   "declare",
   "import",
   "sql",
   "accept",
   "except",
   "rename",
   "primary_key",
   "timezone",
   "and",
   "or",
   "not",
   "asc",
   "desc",
   "by",
   "pick",
   "when",
   "else",
   "then",
   "for",
   "to",
   "exclude",
   "ungroup",
   "all",
]);
const CONSTANTS = new Set(["true", "false", "null"]);

function escapeHtml(s: string): string {
   return s.replace(/[&<>]/g, (c) =>
      c === "&" ? "&amp;" : c === "<" ? "&lt;" : "&gt;",
   );
}

export function highlightMalloy(code: string): string {
   // Order matters: comments and strings are matched before identifiers/numbers
   // so a `//` or digits inside a string/comment aren't re-tokenized. `r'…'` is a
   // Malloy regex literal; `#…` covers tag/doc annotations (grayed as metadata).
   const token =
      /(\/\/[^\n]*|--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/)|(r'(?:[^'\\]|\\.)*'|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")|(\b\d+(?:\.\d+)?\b)|([A-Za-z_]\w*)/g;
   let out = "";
   let last = 0;
   let m: RegExpExecArray | null;
   while ((m = token.exec(code)) !== null) {
      out += escapeHtml(code.slice(last, m.index));
      const text = m[0];
      let cls: string | null = null;
      if (m[1]) cls = "cmt";
      else if (m[2]) cls = "str";
      else if (m[3]) cls = "num";
      else if (m[4]) {
         if (KEYWORDS.has(text)) cls = "kw";
         else if (CONSTANTS.has(text)) cls = "num";
         // A bare identifier immediately followed by "(" reads as a function.
         else if (/^\s*\(/.test(code.slice(m.index + text.length))) cls = "fn";
      }
      out += cls
         ? `<span class="mh-${cls}">${escapeHtml(text)}</span>`
         : escapeHtml(text);
      last = m.index + text.length;
   }
   out += escapeHtml(code.slice(last));
   return out;
}
