// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Texts that tell two readings of Malloy apart: non-ASCII names, a string
 * that crosses a line, a `"""` SQL block with a `%{ … }` in it, a backtick
 * name with an escape, prefixed literals, comments and annotations, and
 * unbalanced brackets. Shared by the lexer characterization specs, which pin
 * what each reader makes of every one.
 */
export const LEXER_CORPUS: Record<string, string> = {
   ascii: "source: orders is t extend { dimension: x is 1 }",
   "non-ASCII source name":
      "source: café is t extend { measure: n is count() }",
   "non-ASCII field name":
      "source: s is t extend { dimension: größe is size }\nsource: u is s",
   "non-ASCII after a name": "source: s is t extend { dimension: aé: is 1 }",
   "CJK name": "source: 注文 is t extend { measure: 件数 is count() }",
   "string crossing a line":
      "source: s is t extend { where: x = 'a\nb' }\nsource: u is s",
   "double-quoted string crossing a line":
      'source: s is t extend { where: x = "a\nb" }\nsource: u is s',
   "sql block": 'source: s is duckdb.sql("""select 1 as x""")\nsource: u is s',
   "sql block with interpolation":
      'source: s is duckdb.sql("""select * from %{ t -> { select: x } } where 1 = 1""")\nsource: u is s',
   "sql block with a brace in SQL":
      'source: s is duckdb.sql("""select \'{\' as x""")\nsource: u is s',
   "backtick name": "source: `my-src` is t extend { dimension: `a b` is 1 }",
   "backtick name with an escape":
      "source: `a\\`b` is t extend { dimension: `c\\`d` is 1 }\nsource: u is `a\\`b`",
   "backtick crossing a line": "source: `a\nb` is t\nsource: u is t",
   "filter literal":
      "source: s is t extend { where: x ~ f'a,b' }\nsource: u is s",
   "regex literal":
      "source: s is t extend { where: x ~ r'a+' }\nsource: u is s",
   "line comments": "-- c\n// d\nsource: s is t // e\nsource: u is s -- f",
   "block comment": "/* a\nb */ source: s is t\nsource: u is s",
   "unclosed block comment": "source: s is t /* never closed\nsource: u is s",
   annotations:
      "#(doc) a doc\n# label\nsource: s is t\n## model\nsource: u is s",
   "unbalanced open": "source: s is t extend { dimension: x is (1 }",
   "unbalanced close": "source: s is t extend { dimension: x is 1 } }",
   "unclosed string": "source: s is t extend { where: x = 'a }",
   "time literal": "source: s is t extend { where: ts > @2021-01-01 10:00 }",
   "number then colon": "query: q is t -> { limit: 10 }",
};
