import { ScaffoldError } from "./errors";

/**
 * The same character rule Publisher enforces on package and environment names
 * (path_safety.ts): letters, digits, ".", "_", "-", not "." or ".." and not
 * starting with ".". Keeping it identical means a name this tool accepts is a
 * name the server will also accept, so the scaffold never produces a package the
 * server then refuses to load.
 */
const SAFE_NAME_RE = /^(?!\.\.?$)(?!\.)[A-Za-z0-9._-]+$/;

/**
 * The longest filename almost every filesystem in use (APFS, ext4, NTFS, HFS+)
 * accepts for a single path component.
 */
const MAX_FILENAME = 255;

/**
 * Publisher's own limit is 255, but this tool also writes
 * `<source-name>.malloy` inside the package directory, and ".malloy" is seven
 * more characters. A 255-character name therefore passed validation and then
 * died on ENAMETOOLONG from `fs.writeFileSync`, after the package directory,
 * publisher.json and the data file were already on disk. Cap the name where the
 * limit can still be explained instead.
 */
const MAX_PACKAGE_NAME = MAX_FILENAME - ".malloy".length;

export function validatePackageName(name: string): void {
   if (!SAFE_NAME_RE.test(name)) {
      // preview(), not the raw name. This is the one message that echoes a name
      // the rule has just refused, so by construction it holds characters
      // outside [A-Za-z0-9._-], and ESC and CR are among them: printed raw, a
      // name can repaint the error line above it into something else. The name
      // reaches here off a command line an agent or a script composes as often
      // as a person types it, and nothing on this path caps its length.
      throw new ScaffoldError(
         `Invalid package name "${preview(name)}": use letters, digits, "-", ` +
            `"_", or "." and do not start with ".".`,
      );
   }
   if (name.length > MAX_PACKAGE_NAME) {
      throw new ScaffoldError(
         `Package name is ${name.length} characters; the limit is ` +
            `${MAX_PACKAGE_NAME}, because the package also holds a ` +
            `"<name>.malloy" model file and a filename cannot exceed ` +
            `${MAX_FILENAME} characters.`,
      );
   }
   // The derived source name can be a few characters longer than the package
   // name (a leading digit gains "_", a Malloy reserved word gains "_source"),
   // so the file that is actually written is the one to measure.
   const modelFile = `${toMalloyIdentifier(name)}.malloy`;
   if (modelFile.length > MAX_FILENAME) {
      throw new ScaffoldError(
         // The name is long by definition here, so it is previewed; modelFile is
         // derived through toMalloyIdentifier, which has already replaced
         // everything outside [A-Za-z0-9_].
         `Package name "${preview(name)}" would need a model file named ` +
            `"${modelFile}", ` +
            `which is ${modelFile.length} characters and cannot be written ` +
            `(the limit is ${MAX_FILENAME}). Shorten the name by ` +
            `${modelFile.length - MAX_FILENAME}.`,
      );
   }
}

/**
 * Publisher's own SAFE_NAME_RE is `{1,255}`, and its REST layer runs that same
 * allowlist over the environment segment of every URL, so a longer name is one
 * the server answers 400 for.
 */
const MAX_ENVIRONMENT_NAME = 255;

/**
 * Validate an environment name that came out of an existing
 * publisher.config.json.
 *
 * Unlike a package name, this one is not typed by the person running the tool:
 * it is read from a config file that ships in repositories (Publisher's own
 * samples call their first environment "examples"), and nothing else in this
 * tool looks at it. It then reaches `--watch-env <name>` in the start and reset
 * scripts written into package.json, the bash fences in AGENTS.md, and the REST
 * URLs the CLI prints. A name holding a shell metacharacter is therefore a
 * command the tool tells the user, or an agent, to run next.
 *
 * Two quieter failures have the same cause and are the reason this is not only a
 * security check: `--watch-env my env` watches an environment named "my", so the
 * server starts, reports serving, and never mounts the package; and a name that
 * is not a string at all renders as "[object Object]".
 *
 * The rule is SAFE_NAME_RE, which is what Publisher enforces on the environment
 * segment of its own URLs (path_safety.ts, applied in server.ts), so nothing
 * this accepts can be refused by the server.
 */
export function validateEnvironmentName(name: unknown): void {
   if (typeof name !== "string") {
      throw new ScaffoldError(
         `publisher.config.json has an environment whose "name" is ` +
            `${name === null ? "null" : typeof name}, not a string. This tool ` +
            `writes that name into the start and reset commands, into ` +
            `AGENTS.md, and into the REST URLs it prints, where it would read ` +
            `as "${String(name)}". Give the environment a name, then run again.`,
      );
   }
   if (!SAFE_NAME_RE.test(name)) {
      throw new ScaffoldError(
         `Invalid environment name "${preview(name)}" in ` +
            `publisher.config.json${describeUnsafeCharacter(name)}: use ` +
            `letters, digits, "-", "_", or "." and do not start with ".". ` +
            `This tool writes the environment name into the ` +
            `--watch-env argument of the start and reset commands and into the ` +
            `shell blocks in AGENTS.md, so anything outside that set would be ` +
            `run as part of those commands. Publisher rejects the same names ` +
            `on its own URLs. Rename the environment, then run again.`,
      );
   }
   if (name.length > MAX_ENVIRONMENT_NAME) {
      throw new ScaffoldError(
         `Environment name "${preview(name)}" in publisher.config.json is ` +
            `${name.length} characters; the limit is ${MAX_ENVIRONMENT_NAME}, ` +
            `which is what Publisher accepts in a URL. Rename the environment, ` +
            `then run again.`,
      );
   }
}

/**
 * Keep a hostile or accidental 10KB name from filling the terminal.
 *
 * Exported because the CLI quotes strings read off disk that are under no length
 * rule at all: a `scripts.start` entry out of somebody else's package.json can
 * be any length, and printable() on its own caps nothing.
 */
export function preview(value: string, max = 60): string {
   const shortened = value.length > max ? `${value.slice(0, max)}...` : value;
   return printable(shortened);
}

/**
 * Render a string that came out of the workspace so that printing it can only
 * add characters to the terminal, never commands to it.
 *
 * Everything this tool echoes back has been read off disk in a directory the
 * user may have cloned rather than written: an environment name out of
 * publisher.config.json, a start script out of somebody else's package.json. A
 * string holding ESC can move the cursor, repaint the line above it, and set a
 * colour that outlives the run; a lone CR returns to column zero and lets the
 * next characters overwrite what was already there. Both are how a line reading
 * "Overwrote these:" is turned into a green check the tool never printed.
 *
 * Control characters are therefore shown rather than sent. The escaping is the
 * same idea `describeUnsafeCharacter` uses to name a character a terminal will
 * not show, applied to a whole string, and it is idempotent: the output holds
 * only characters this leaves alone.
 */
export function printable(value: string): string {
   let out = "";
   for (const character of value) {
      out += isControl(character) ? escapeCharacter(character) : character;
   }
   return out;
}

/**
 * C0 controls, DEL, the C1 range, the two Unicode line breaks, and the bidi
 * formatting characters.
 *
 * The bidi set is here for the same reason as the rest: it changes what the
 * terminal shows without adding a character the reader can see. U+202E
 * (RIGHT-TO-LEFT OVERRIDE) and the isolates reorder the run that follows them,
 * so a decline reason quoting a `scripts.start` off somebody else's
 * package.json can be made to read as a different sentence than the one this
 * tool composed — the same "a line that reads as a promise it did not make"
 * failure the escaping above exists to prevent, reached by reordering rather
 * than repainting. They are invisible on their own, so showing the escape is
 * the only way a reader learns they were there at all.
 */
function isControl(character: string): boolean {
   const code = character.codePointAt(0) as number;
   return (
      code < 0x20 ||
      code === 0x7f ||
      (code >= 0x80 && code <= 0x9f) ||
      code === 0x2028 ||
      code === 0x2029 ||
      // LRM, RLM, and ALM: directional marks.
      code === 0x200e ||
      code === 0x200f ||
      code === 0x061c ||
      // LRE, RLE, PDF, LRO, RLO: the embedding and override pair.
      (code >= 0x202a && code <= 0x202e) ||
      // LRI, RLI, FSI, PDI: the isolates that replaced them.
      (code >= 0x2066 && code <= 0x2069)
   );
}

function escapeCharacter(character: string): string {
   switch (character) {
      case "\n":
         return "\\n";
      case "\r":
         return "\\r";
      case "\t":
         return "\\t";
      default:
         return `\\u${codePointHex(character)}`;
   }
}

function codePointHex(character: string): string {
   const code = character.codePointAt(0) as number;
   return code.toString(16).toUpperCase().padStart(4, "0");
}

/**
 * Name the first character that fails the rule. Worth saying, because the
 * characters that get a name here are often the ones a terminal does not show:
 * a space, a tab, a stray newline out of a hand-edited config.
 */
function describeUnsafeCharacter(name: string): string {
   for (const character of name) {
      if (/[A-Za-z0-9._-]/.test(character)) {
         continue;
      }
      const code = character.codePointAt(0) as number;
      const shown =
         character === " "
            ? "a space"
            : character.trim() === "" || code < 0x20 || code === 0x7f
              ? `U+${codePointHex(character)}`
              : `"${printable(character)}"`;
      return ` (the first character it cannot hold is ${shown})`;
   }
   // Every character is allowed, so the name failed on the other half of the
   // rule: it is "." or ".." or it starts with ".".
   return "";
}

/**
 * Names that cannot be used as a bare Malloy source name. Two disjoint hazards,
 * both fatal to the model the server then tries to load:
 *
 * - Grammar keywords, rejected at parse time with "'<word>' is a reserved word, so
 *   to use it as a name you must quote it".
 * - Malloy standard function names, rejected later, once a dialect is attached,
 *   with "Cannot redefine '<word>'". A parse-only sweep does not see these, which
 *   is why they were missing for so long.
 *
 * A derived source name landing on either is suffixed (below) rather than quoted,
 * so the source stays queryable by a plain name through the REST/MCP query API,
 * which composes `run: <source> -> <view>` without quoting.
 *
 * This list is derived from the installed @malloydata/malloy rather than curated
 * by hand, and names.spec.ts re-derives both sets from that package on every run.
 * A Malloy upgrade that adds a keyword or a standard function therefore fails the
 * suite instead of silently shipping packages the server cannot compile. To
 * regenerate: run `bun test src/names.spec.ts` and add the words it names.
 */
const MALLOY_RESERVED = new Set([
   // Grammar keywords (MalloyLexer).
   "all",
   "and",
   "as",
   "asc",
   "avg",
   "boolean",
   "by",
   "case",
   "cast",
   "compose",
   "count",
   "date",
   "day",
   "desc",
   "distinct",
   "else",
   "end",
   "exclude",
   "export",
   "extend",
   "false",
   "filter",
   "for",
   "from",
   "full",
   "has",
   "hour",
   "import",
   "in",
   "include",
   "inner",
   "internal",
   "is",
   "json",
   "left",
   "like",
   "max",
   "min",
   "minute",
   "month",
   "not",
   "now",
   "null",
   "number",
   "on",
   "or",
   "pick",
   "private",
   "public",
   "quarter",
   "right",
   "second",
   "source",
   "sql",
   "string",
   "sum",
   "table",
   "then",
   "this",
   "timestamp",
   "timestamptz",
   "to",
   "true",
   "virtual",
   "week",
   "when",
   "with",
   "year",

   // Standard function names (MALLOY_STANDARD_FUNCTIONS). Dialect-independent:
   // every dialect inherits the whole set, so one list covers every warehouse.
   "abs",
   "acos",
   "ascii",
   "asin",
   "atan",
   "atan2",
   "avg_moving",
   "byte_length",
   "ceil",
   "chr",
   "coalesce",
   "concat",
   "cos",
   "dense_rank",
   "div",
   "ends_with",
   "exp",
   "first_value",
   "floor",
   "greatest",
   "ifnull",
   "is_inf",
   "is_nan",
   "lag",
   "last_value",
   "lead",
   "least",
   "length",
   "ln",
   "log",
   "lower",
   "ltrim",
   "max_cumulative",
   "max_window",
   "min_cumulative",
   "min_window",
   "nullif",
   "pi",
   "pow",
   "rand",
   "rank",
   "regexp_extract",
   "replace",
   "round",
   "row_number",
   "rtrim",
   "sign",
   "sin",
   "sql_boolean",
   "sql_date",
   "sql_number",
   "sql_string",
   "sql_timestamp",
   "sqrt",
   "starts_with",
   "stddev",
   "string_repeat",
   "strpos",
   "substr",
   "sum_cumulative",
   "sum_moving",
   "sum_window",
   "tan",
   "trim",
   "trunc",
   "unicode",
   "upper",
]);

/**
 * Turn a package name into a valid Malloy source identifier. Package names allow
 * "-" and "." and may start with a digit; Malloy identifiers do not, so replace
 * anything outside [A-Za-z0-9_] with "_" and prefix a leading digit. A name that
 * lands on a Malloy reserved word is suffixed. "sales" stays "sales";
 * "retail-sales" becomes "retail_sales"; "2024-data" becomes "_2024_data";
 * "count" becomes "count_source".
 */
export function toMalloyIdentifier(name: string): string {
   let identifier = name.replace(/[^A-Za-z0-9_]/g, "_");
   if (/^[0-9]/.test(identifier)) {
      identifier = `_${identifier}`;
   }
   if (MALLOY_RESERVED.has(identifier.toLowerCase())) {
      identifier = `${identifier}_source`;
   }
   return identifier;
}
