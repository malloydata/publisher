// Reading a Malloy result envelope: the rows, and the field metadata that says
// how to present them.
//
// `Publisher.query` already returns plain row objects, which is all most pages
// need. This page calls `Publisher.queryFull` instead, because the envelope also
// carries each field's annotations, and those are where the model states what a
// column is called (`# label`), how its numbers read (`# currency`, `# percent`,
// `# number`), and whether clicking it drills (`# drill`). Reading them means the
// page never keeps a second copy of any of that: add a `# drill` to a dimension
// in storefront.malloy, reload the package, and a table here becomes clickable
// with no edit to this directory.
//
// The tag reading below is a small subset of MOTLY, enough for the tags this
// page acts on. The server and the React SDK use Malloy's real parser; a
// no-build page cannot import it, so this is the honest minimum rather than a
// reimplementation.

/** Pull the value out of one Malloy cell, whatever kind it is. */
function cellValue(cell) {
   if (!cell || typeof cell !== "object") return null;
   switch (cell.kind) {
      case "string_cell":
         return cell.string_value;
      case "number_cell":
         return cell.number_value;
      case "boolean_cell":
         return cell.boolean_value;
      case "date_cell":
         return cell.date_value;
      case "timestamp_cell":
         return cell.timestamp_value;
      case "null_cell":
         return null;
      default:
         // A nested table (`record_cell` / `array_cell`) is not something this
         // page renders; a tile that needs one should query it as its own tile.
         return null;
   }
}

const annotationTexts = (field) =>
   (field.annotations ?? []).map((a) => String(a.value ?? ""));

/** `# label="Category"` -> `Category`. */
function readLabel(texts) {
   for (const text of texts) {
      const match = /^#\s+label\s*=\s*"([^"]*)"/.exec(text);
      if (match) return match[1];
   }
   return undefined;
}

/**
 * Which formatter a field asks for. `# number=` carries a Malloy/Excel-style
 * pattern; the only two this page distinguishes are `id` (leave the digits
 * alone) and anything with a decimal place in it.
 */
function readFormat(texts, name) {
   for (const text of texts) {
      if (/^#\s+currency\b/.test(text)) return "currency";
      if (/^#\s+percent\b/.test(text)) return "percent";
      const number = /^#\s+number\s*=\s*"?([^"\s]+)"?/.exec(text);
      if (number) return number[1] === "id" ? "id" : "decimal";
   }
   // Not a tag: a month-truncated timestamp is a date the page shows as Jan 2024.
   return name.endsWith("_month") ? "month" : undefined;
}

/**
 * A measure carries `calculation` in Malloy's own annotation. It is how the
 * Console tells a dimension from an aggregate, and drill needs the distinction:
 * a `# drill` tag inherited by a total must not seed a filter, because the total
 * is not the value that was clicked.
 */
const readIsAggregate = (texts) =>
   texts.some((text) => /^#\(malloy\)[^\n]*\bcalculation\b/.test(text));

/**
 * `# drill { to=["category", "self"] given=CATEGORY }` -> the destinations and
 * the given to seed. A single destination may be written unbracketed
 * (`to=overview`), and `given=` is optional: it defaults to the field name
 * upper-cased, the same default the server lints against.
 */
function readDrill(texts, name) {
   for (const text of texts) {
      const drill = /^#\s+drill\s*\{([\s\S]*?)\}/.exec(text);
      if (!drill) continue;
      const body = drill[1];
      const list = /to\s*=\s*\[([^\]]*)\]/.exec(body);
      const single = /to\s*=\s*("?)([\w-]+)\1/.exec(body);
      const to = list
         ? list[1]
              .split(",")
              .map((part) => part.trim().replace(/^"|"$/g, ""))
              .filter(Boolean)
         : single
           ? [single[2]]
           : [];
      if (to.length === 0) return undefined;
      const given = /given\s*=\s*"?(\w+)"?/.exec(body);
      return { to, given: given ? given[1] : name.toUpperCase() };
   }
   return undefined;
}

/**
 * Turn one envelope into `{ fields, rows }`, where each field knows its own
 * heading, format, and drill, and each row is a plain object keyed by field name.
 */
export function readResult(envelope) {
   const fields = (envelope?.schema?.fields ?? []).map((field) => {
      const texts = annotationTexts(field);
      return {
         name: field.name,
         label: readLabel(texts) ?? field.name,
         format: readFormat(texts, field.name),
         isAggregate: readIsAggregate(texts),
         drill: readDrill(texts, field.name),
      };
   });

   const records = envelope?.data?.array_value ?? [];
   const rows = records.map((record) => {
      const cells = record.record_value ?? [];
      const row = {};
      fields.forEach((field, i) => {
         row[field.name] = cellValue(cells[i]);
      });
      return row;
   });

   return { fields, rows };
}
