// Pure functions: no DOM, no globals, no Publisher. Everything here is a value
// in and a value out, which is what makes it the one file `node --test` can
// cover directly.

const CURRENCY = new Intl.NumberFormat(undefined, {
   style: "currency",
   currency: "USD",
   maximumFractionDigits: 0,
});
const CURRENCY_CENTS = new Intl.NumberFormat(undefined, {
   style: "currency",
   currency: "USD",
   minimumFractionDigits: 2,
   maximumFractionDigits: 2,
});
const PERCENT = new Intl.NumberFormat(undefined, {
   style: "percent",
   minimumFractionDigits: 1,
   maximumFractionDigits: 1,
});
const DECIMAL = new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 });
const INTEGER = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

/** Malloy hands large integers back as strings, so coerce before formatting. */
const toNumber = (value) => {
   const n = typeof value === "number" ? value : Number(value);
   return Number.isFinite(n) ? n : null;
};

export const usd = (value, cents = false) => {
   const n = toNumber(value);
   return n === null ? "—" : (cents ? CURRENCY_CENTS : CURRENCY).format(n);
};

export const percent = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : PERCENT.format(n);
};

export const decimal = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : DECIMAL.format(n);
};

export const integer = (value) => {
   const n = toNumber(value);
   return n === null ? "—" : INTEGER.format(n);
};

/**
 * Format a cell the way the field's own tags ask for, so the page never carries
 * a second opinion about how a number reads. `# currency` and `# percent` come
 * from the model; `id` is Malloy's "this is an identifier, do not group the
 * digits" format, which is why a customer id renders as 884 and not 884.
 */
export function formatValue(value, format) {
   if (value === null || value === undefined) return "—";
   switch (format) {
      case "currency":
         return usd(value);
      case "percent":
         return percent(value);
      case "id":
         return String(value);
      case "decimal":
         return decimal(value);
      case "month":
         return monthLabel(value);
      case "integer":
         return integer(value);
      default:
         return typeof value === "number" || !Number.isNaN(Number(value))
            ? integer(value)
            : String(value);
   }
}

/** A month-truncated timestamp comes back as an ISO string: keep YYYY-MM. */
export const monthKey = (value) => String(value ?? "").slice(0, 7);

export function monthLabel(value) {
   const key = monthKey(value);
   if (!/^\d{4}-\d{2}$/.test(key)) return String(value ?? "—");
   const [year, month] = key.split("-");
   const name = [
      "Jan",
      "Feb",
      "Mar",
      "Apr",
      "May",
      "Jun",
      "Jul",
      "Aug",
      "Sep",
      "Oct",
      "Nov",
      "Dec",
   ][Number(month) - 1];
   return `${name} ${year}`;
}

const MONTHS_SHORT = [
   "Jan",
   "Feb",
   "Mar",
   "Apr",
   "May",
   "Jun",
   "Jul",
   "Aug",
   "Sep",
   "Oct",
   "Nov",
   "Dec",
];

/** `month(created_at)` is a number 1-12, not a date. */
export const monthOfYearLabel = (n) => MONTHS_SHORT[Number(n) - 1] ?? String(n);

/** Enough pluralizing for a control's "All categories" / "All brands" option. */
export const plural = (word) => {
   const text = String(word ?? "");
   if (/[^aeiou]y$/i.test(text)) return `${text.slice(0, -1)}ies`;
   if (/(s|x|z|ch|sh)$/i.test(text)) return `${text}es`;
   return `${text}s`;
};

/**
 * "category_detail" -> "Category detail". The same transform the Console applies
 * to a drill menu item, so a destination reads the same in both places.
 */
export function humanizeSlug(slug) {
   const words = String(slug ?? "")
      .replace(/[-_]+/g, " ")
      .trim();
   return words ? words[0].toUpperCase() + words.slice(1) : "";
}

/**
 * Encode a clicked cell value as the filter expression a `filter<T>` given
 * takes, mirroring the Console so a drill out of this page and a drill out of a
 * dashboard seed the same value.
 *
 * A comma separates alternatives in filter syntax, so a value containing one has
 * to be quoted or "Ben & Jerry, Inc" would filter for two brands. A double quote
 * or a backslash has to be quoted *and* escaped, since either one otherwise ends
 * the quoting early and turns the rest of the value into more filter syntax. An
 * apostrophe needs nothing. Returns undefined for anything that cannot be a
 * filter, which is the signal not to offer a drill at all.
 *
 * Kept in step with the SDK's `encodeFilterList`, deliberately down to the
 * escaping order: backslashes first, so the ones this adds are not escaped again.
 */
export function encodeFilterValue(value) {
   if (value === null || value === undefined) return undefined;
   if (typeof value === "boolean") return String(value);
   if (typeof value === "number")
      return Number.isFinite(value) ? String(value) : undefined;
   if (value instanceof Date)
      return Number.isNaN(value.getTime())
         ? undefined
         : value.toISOString().slice(0, 10);
   if (typeof value !== "string") return undefined;
   return /[,"\\]/.test(value) || value.trim() !== value || value === ""
      ? `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`
      : value;
}

/** A number slider sends a filter expression, not a bare number. */
export const atLeast = (n) => (Number(n) > 0 ? `>= ${Number(n)}` : "");

/** `">= 50"` -> 50, for putting a slider back where a URL says it was. */
export function readAtLeast(filter) {
   const match = /(-?\d+(?:\.\d+)?)/.exec(String(filter ?? ""));
   return match ? Number(match[1]) : 0;
}

/**
 * The inverse of encodeFilterValue for one value: drop the quoting.
 *
 * Escapes come off in one left-to-right pass rather than one replace per
 * character, so a `\\` becomes a single backslash instead of being read as the
 * start of an escape for whatever followed it.
 */
export const decodeFilterValue = (value) =>
   String(value ?? "")
      .trim()
      .replace(/^"(.*)"$/s, "$1")
      .replace(/\\(.)/g, "$1");

/**
 * Split a filter list on its commas, leaving commas that are inside quotes
 * alone, so `Nike, "Ben & Jerry, Inc"` reads as two brands and not three.
 */
export function decodeFilterList(filter) {
   const text = String(filter ?? "");
   const parts = [];
   let current = "";
   let quoted = false;
   for (let i = 0; i < text.length; i++) {
      const char = text[i];
      if (char === "\\" && quoted) {
         current += char + (text[++i] ?? "");
      } else if (char === '"') {
         quoted = !quoted;
         current += char;
      } else if (char === "," && !quoted) {
         parts.push(current);
         current = "";
      } else {
         current += char;
      }
   }
   parts.push(current);
   return parts.map(decodeFilterValue).filter((part) => part !== "");
}

/** `@2023-01-01` and `f''` are Malloy literals; a control needs the value inside. */
export function readGivenDefault(literal) {
   const text = String(literal ?? "");
   if (/^f''$/.test(text)) return "";
   const filter = /^f'(.*)'$/s.exec(text);
   if (filter) return filter[1];
   return text.replace(/^@/, "");
}
