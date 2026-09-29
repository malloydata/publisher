// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// GENERATED from Credible's indexing/packages/packages/llms/prompts/entity_keyphrase.txt by
// the port script; do not edit by hand. The wording is kept exactly because a
// keyphrase or summary here should mean what one there means. A change goes in
// as a new prompt version, never an edit: the version is part of every cache
// key and every recorded run.

export const KEYPHRASE_PROMPT_VERSIONS = ["v1"] as const;

export const KEYPHRASE_ROLE = `You distill a single concise retrieval keyphrase for a field (column / dimension / measure / view / join) in a Malloy semantic model. This keyphrase is what gets embedded for vector search; a user's natural-language query will be cosine-matched against it. Always respond with just a JSON formatted answer, no other text.`;

/** The single-field prompt, verbatim. */
export const KEYPHRASE_BODY = `You are given a single field from the Malloy source "##SOURCE##".

Field name: ##NAME##
Field type: ##TYPE##
Data type: ##DTYPE##
Description: ##DESCRIPTION##

Sibling fields (schema context):
##SCHEMA##

Field code:
##CODE##

<END_OF_ENTITY_CODE>

Background context (do NOT mention any of these in the keyphrase; they are for your understanding only):
* Model file path: ##MODEL_FILE_PATH##

Note: the description is OPTIONAL and may be empty. If a description is present, treat it as the primary signal and follow every rule below that references the description. If the description is empty or missing, ground the keyphrase in the remaining inputs only — the field name, field type, data type, field code, and sibling fields in the schema — and do NOT invent semantics that those inputs do not assert. Every faithfulness rule below still applies: no invented units, purpose, classification, scope, enum values, or computation, regardless of whether a description is present.

Examples of grounding from name + schema + code only (description empty):
* Name \`email\`, sibling fields include \`first_name\`, \`last_name\`, \`user_id\` → "Email address of the user." (the noun "user" is grounded by sibling fields; do NOT add "for communication" or "personal".)
* Name \`created_at\`, type \`timestamp\`, source is \`orders\` → "Order created timestamp." (do NOT add "in UTC" or "in seconds" — no unit appears in the inputs.)
* Name \`inventory_item_id\`, code \`references inventory_items.id\` → "Joins inventory_items by inventory_item_id." (FK hint comes from the code, not invented.)
* Name \`total_revenue\`, code \`sum(sale_price)\` → "Total revenue across line items." (do NOT add "in USD" — no currency appears in the inputs.)
* Name \`status\`, code lists enum values \`'pending', 'shipped', 'delivered'\` → "Order fulfillment status." (infer the concept from values; do not list them.)
* Name \`country\`, no description, no clarifying siblings → "Country." (a short faithful keyphrase beats an invented clarifier when the schema gives no grounding.)

Your task: produce ONE concise retrieval keyphrase that captures what this field is, suitable for matching the short semantic search phrases an agent sends to retrieval.

### Length target:

* For columns, dimensions, measures, and joins: use **3-10 words** whenever possible.
* For views: use **3-14 words** when the view's grain, top-N shape, dashboard scope, or trend/breakdown needs that space.
* Short and faithful beats long and padded. Do not expand a simple field just to hit a word count.
* You may exceed the target only when preserving retrieval-critical null semantics, FK / join hints, or verbatim unit tokens from the description.

### Rules:

* **FAITHFULNESS — only state what the inputs assert.** The keyphrase must contain only facts that are literally present in the description, field code, schema, or sibling fields you were given. Do NOT add anything else, even when your prior knowledge says it would be true:
  - No unit of measure of any kind unless verbatim in the inputs. This covers currency ("USD", "$", "in dollars"), percentage ("percent", "%"), weight ("kg", "lbs", "pounds", "ounces", "grams"), length ("meters", "miles", "feet", "km"), time ("seconds", "minutes", "ms", "hours"), volume ("liters", "gallons"), temperature ("Celsius", "Fahrenheit"), and anything else. We don't know the unit unless the inputs say it.
  - No purpose ("for identification", "for tracking", "for analysis", "used to filter").
  - No classification ("personal", "PII", "biometric", "biological", "sensitive").
  - No domain or storage context ("ecommerce", "retail", "in the customer database", "within the system", "across the platform").
  - No invented quantifiers ("active", "registered", "currently", "specific", "individual").
  - No computation restatement ("sum of", "minus", "divided by", "calculated as").
  - No invented enum values, source names, table names, or column names not in the inputs.
  If a fact is not literally in the description, code, or schema, it does not belong in the keyphrase. A short faithful keyphrase is always better than a longer invented one.
* Lead with the most discriminating noun phrase. Examples of acceptable openers:
  - "Wholesale cost of one inventory item."
  - "Customer-placed-order timestamp."
  - "Order fulfillment status." (inferred concept from an enum; do not list values)
  - "Customer lifetime spend, excluding cancelled and returned orders."
  - "Joins inventory_items by inventory_item_id."
* Prefer compact noun-phrase style over sentence expansion:
  - Good: "Total sales revenue."
  - Good: "Order returned timestamp; null if not returned."
  - Good: "Product brand."
  - Good: "Monthly sales trend by order date."
  - Bad: "Total sales revenue generated from all customer orders within the ecommerce system."
  - Bad: "Date and time when the order item was returned in the database table."
* Do NOT use the templated opener "A \`<type>\` representing/indicating/containing the \`<name>\`". This pattern is banned because it adds the field's data type and name back into the keyphrase that already sit next to it in the schema.
* Do NOT begin with "A string", "A number", "A numeric", "A monetary amount", "A date", "A timestamp", "A boolean", "A view", "A join", "A dimension", "A measure" unless that type word is essential to disambiguate the field from siblings (rare).
* The word "entity" is forbidden.
* PRESERVE retrieval-critical signals from the description if they are present:
  - **Enum values.** If the description lists the legal values of a categorical field, do NOT carry those values literally into the keyphrase — listing values adds noise to the embedding without helping retrieval. Instead, INFER from the values what the field semantically represents and write the keyphrase from that inference. The values are evidence; the keyphrase is the inferred concept. Examples:
    - "Gender: Male or Female" → "Customer gender."
    - "Status: Complete, Cancelled, Returned, Processing, Shipped" → "Order fulfillment status."
    - "Department: Electronics, Apparel, Home, Garden, Toys, Books, Beauty" → "Product department."
  - **Foreign key / join hints.** If the description says "FK to <table>" or "joins to <source>", keep that hint. Do NOT propagate "(denormalized from <source>)" — denormalization provenance is not a retrieval signal and should stay in the description only.
  - **Units of measure.** **If the description does not state the unit, the keyphrase MUST NOT include a unit. If the description states the unit, keep it verbatim.** No exceptions, no inference from field name, data type, or domain. Applies to every kind of unit — currency, percentage, weight, length, time, volume, temperature, or any other measurement. Examples:
    - Description "Profit margin on this item (sale price minus cost)" → "Profit margin per item." (NOT "Profit margin in USD per item.")
    - Description "Total gross margin (revenue minus cost of goods sold)" → "Total gross margin." (NOT "Total gross margin in USD." — neither "USD" nor "$" appears in the description.)
    - Description "Average gross margin per line item" → "Average gross margin per line item." (NOT "Average gross margin per line item in USD.")
    - Description "Weight of the box" → "Weight of the box." (NOT "Weight of the box in lbs." or "Weight of the box in kg.")
    - Description "Total revenue from all line items" → "Total revenue from all line items." (NOT "Total revenue in USD.")
    - Description "Wholesale cost of this inventory item" → "Wholesale cost of an item." (NOT "Wholesale cost in USD.")
    - Description "Order delivery duration" → "Order delivery duration." (NOT "Order delivery duration in days.")
    - Description "Customer lifetime spend in USD" → "Customer lifetime spend in USD." (USD appears verbatim, so keep it.)
  - **Computation / formula tokens are NOT a retrieval signal.** Do NOT include tokens like "sum of", "minus", "divided by", "ratio of", "calculated as", or any restatement of the formula. Computation belongs in the description, not the keyphrase. Describe what the field IS (e.g., "Profit margin per item."), not how it is computed (e.g., NOT "Profit margin, calculated as sale price minus cost.").
* Distill ONLY the parts of the description that help a stranger recognize this field for retrieval. Drop internal process steps, ticket numbers, references like "we use this for X", and other operational noise.
* Drop CRM/marketing/SQL-implementation filler that appeared in the description: "lifecycle", "tenure", "primary key", "lookup table", "behavioral patterns", "across the platform", "within the system".
* Do NOT classify the data or invent its purpose. Forbidden vocabulary unless it appears verbatim in the description, field code, or schema you were given: "personal", "personally identifiable", "PII", "biometric", "biological", "biological sex", "sensitive", "private", "for identification", "for communication", "for tracking", "for analysis", "for reporting", "for storage", "stored within", "stored in the database", "in the database table", "in the customer database", "information source", "data source". If the description only says "Gender: Male or Female", the keyphrase must NOT add "biological sex" or any other classification claim. State only what the description / code / schema asserts.
* Do NOT use padding doublings or stacked quantifiers: "single individual", "individual specific", "specific specific", "the individual user", "each individual record", "particular specific", "Total number of unique", "the total count of all individual". Pick ONE quantifier; drop the redundant one.
* **IMPORTANT: Trivial \`count()\` measures.** If the field is a simple count measure (name like \`user_count\`, \`product_count\`, \`order_count\`, \`inventory_item_count\`, etc.) the keyphrase MUST be a short "Count of \`<plural-noun>\`." form, e.g. "Count of users.", "Count of products.", "Count of orders.". You MUST NOT add scope qualifiers like "currently registered", "currently active", "available within", or "who have placed orders" unless that qualifier is present verbatim in the description / field code. The \`<plural-noun>\` is the entity being counted (users / products / orders / items), NOT the source name.
* **IMPORTANT: Banned source/scope filler patterns.** The keyphrase must NOT name or paraphrase the source, package, or model. In particular, the source name "##SOURCE##" — and any other source name from the schema — must NOT appear in the keyphrase, in any form (literal, lowercased, or as an adjective like "##SOURCE## platform" / "##SOURCE## system" / "##SOURCE## catalog" / "##SOURCE## database" / "##SOURCE## data source"). Also banned are generic scope-filler clauses regardless of source name:
  - "within the system platform"
  - "within the current dataset or filtered view"
  - "in the customer database" / "in the inventory database" / "in the database table"
  - "associated with the registered user account in the system"
  - "for shipping and billing purposes"
  - "stored within … the product catalog"
  If you find yourself appending one of these, drop the trailing scope clause entirely; a short keyphrase that omits scope is better than one that invents or names scope.
* **Trivial common-noun fields.** If the field's name is itself a common English noun (e.g. \`country\`, \`city\`, \`email\`, \`first_name\`, \`last_name\`, \`brand\`, \`age\`, \`latitude\`, \`longitude\`, \`zip\`) and the description is a near-synonym of the name (e.g. "Email address", "Country", "Brand name", "First name"), your keyphrase MUST:
  1. Preserve the literal name token in natural-language form. The keyphrase for \`email\` MUST contain the word "email"; for \`country\` MUST contain "country"; for \`brand\` MUST contain "brand"; etc.
  2. You MAY add ONE short clarifier ONLY if it is grounded in the source / schema / code AND you are confident it is correct (e.g. "email address of the customer", "user's home country", "first name of the registered user", "brand name of the product"). The clarifier should add real retrieval value (the noun the field belongs to: customer / user / product / order).
  3. You MUST NOT invent purpose ("for identification"), classification ("personal", "biometric", "biological"), storage details ("stored within…the database table"), or unstated semantics. If the schema doesn't say it, don't say it.
  4. If you can't think of a confidently grounded clarifier, return the description verbatim or close to it. A short faithful keyphrase is better than a long invented one.
* Do NOT name the source, the model file path, or any file/path. Do NOT use markdown.
* The keyphrase must be one sentence. End with a single period.

Respond only with a JSON string with the following structure:

{
    "keyphrase": "<one short distilled retrieval keyphrase>"
}`;

/**
 * The batched variant: the same rules applied to several fields of one source
 * in one call. It is built from the verbatim text above by swapping only the
 * single-field block for a list of them and the reply shape for an array, so
 * the rules a batch is held to are the rules a single field is held to.
 */
const BATCH_HEAD = ``;
const BATCH_MID = `

Background context (do NOT mention any of these in the keyphrase; they are for your understanding only):
* Model file path: ##MODEL_FILE_PATH##

Note: the description is OPTIONAL and may be empty. If a description is present, treat it as the primary signal and follow every rule below that references the description. If the description is empty or missing, ground the keyphrase in the remaining inputs only — the field name, field type, data type, field code, and sibling fields in the schema — and do NOT invent semantics that those inputs do not assert. Every faithfulness rule below still applies: no invented units, purpose, classification, scope, enum values, or computation, regardless of whether a description is present.

Examples of grounding from name + schema + code only (description empty):
* Name \`email\`, sibling fields include \`first_name\`, \`last_name\`, \`user_id\` → "Email address of the user." (the noun "user" is grounded by sibling fields; do NOT add "for communication" or "personal".)
* Name \`created_at\`, type \`timestamp\`, source is \`orders\` → "Order created timestamp." (do NOT add "in UTC" or "in seconds" — no unit appears in the inputs.)
* Name \`inventory_item_id\`, code \`references inventory_items.id\` → "Joins inventory_items by inventory_item_id." (FK hint comes from the code, not invented.)
* Name \`total_revenue\`, code \`sum(sale_price)\` → "Total revenue across line items." (do NOT add "in USD" — no currency appears in the inputs.)
* Name \`status\`, code lists enum values \`'pending', 'shipped', 'delivered'\` → "Order fulfillment status." (infer the concept from values; do not list them.)
* Name \`country\`, no description, no clarifying siblings → "Country." (a short faithful keyphrase beats an invented clarifier when the schema gives no grounding.)

Your task: produce ONE concise retrieval keyphrase that captures what this field is, suitable for matching the short semantic search phrases an agent sends to retrieval.

### Length target:

* For columns, dimensions, measures, and joins: use **3-10 words** whenever possible.
* For views: use **3-14 words** when the view's grain, top-N shape, dashboard scope, or trend/breakdown needs that space.
* Short and faithful beats long and padded. Do not expand a simple field just to hit a word count.
* You may exceed the target only when preserving retrieval-critical null semantics, FK / join hints, or verbatim unit tokens from the description.

### Rules:

* **FAITHFULNESS — only state what the inputs assert.** The keyphrase must contain only facts that are literally present in the description, field code, schema, or sibling fields you were given. Do NOT add anything else, even when your prior knowledge says it would be true:
  - No unit of measure of any kind unless verbatim in the inputs. This covers currency ("USD", "$", "in dollars"), percentage ("percent", "%"), weight ("kg", "lbs", "pounds", "ounces", "grams"), length ("meters", "miles", "feet", "km"), time ("seconds", "minutes", "ms", "hours"), volume ("liters", "gallons"), temperature ("Celsius", "Fahrenheit"), and anything else. We don't know the unit unless the inputs say it.
  - No purpose ("for identification", "for tracking", "for analysis", "used to filter").
  - No classification ("personal", "PII", "biometric", "biological", "sensitive").
  - No domain or storage context ("ecommerce", "retail", "in the customer database", "within the system", "across the platform").
  - No invented quantifiers ("active", "registered", "currently", "specific", "individual").
  - No computation restatement ("sum of", "minus", "divided by", "calculated as").
  - No invented enum values, source names, table names, or column names not in the inputs.
  If a fact is not literally in the description, code, or schema, it does not belong in the keyphrase. A short faithful keyphrase is always better than a longer invented one.
* Lead with the most discriminating noun phrase. Examples of acceptable openers:
  - "Wholesale cost of one inventory item."
  - "Customer-placed-order timestamp."
  - "Order fulfillment status." (inferred concept from an enum; do not list values)
  - "Customer lifetime spend, excluding cancelled and returned orders."
  - "Joins inventory_items by inventory_item_id."
* Prefer compact noun-phrase style over sentence expansion:
  - Good: "Total sales revenue."
  - Good: "Order returned timestamp; null if not returned."
  - Good: "Product brand."
  - Good: "Monthly sales trend by order date."
  - Bad: "Total sales revenue generated from all customer orders within the ecommerce system."
  - Bad: "Date and time when the order item was returned in the database table."
* Do NOT use the templated opener "A \`<type>\` representing/indicating/containing the \`<name>\`". This pattern is banned because it adds the field's data type and name back into the keyphrase that already sit next to it in the schema.
* Do NOT begin with "A string", "A number", "A numeric", "A monetary amount", "A date", "A timestamp", "A boolean", "A view", "A join", "A dimension", "A measure" unless that type word is essential to disambiguate the field from siblings (rare).
* The word "entity" is forbidden.
* PRESERVE retrieval-critical signals from the description if they are present:
  - **Enum values.** If the description lists the legal values of a categorical field, do NOT carry those values literally into the keyphrase — listing values adds noise to the embedding without helping retrieval. Instead, INFER from the values what the field semantically represents and write the keyphrase from that inference. The values are evidence; the keyphrase is the inferred concept. Examples:
    - "Gender: Male or Female" → "Customer gender."
    - "Status: Complete, Cancelled, Returned, Processing, Shipped" → "Order fulfillment status."
    - "Department: Electronics, Apparel, Home, Garden, Toys, Books, Beauty" → "Product department."
  - **Foreign key / join hints.** If the description says "FK to <table>" or "joins to <source>", keep that hint. Do NOT propagate "(denormalized from <source>)" — denormalization provenance is not a retrieval signal and should stay in the description only.
  - **Units of measure.** **If the description does not state the unit, the keyphrase MUST NOT include a unit. If the description states the unit, keep it verbatim.** No exceptions, no inference from field name, data type, or domain. Applies to every kind of unit — currency, percentage, weight, length, time, volume, temperature, or any other measurement. Examples:
    - Description "Profit margin on this item (sale price minus cost)" → "Profit margin per item." (NOT "Profit margin in USD per item.")
    - Description "Total gross margin (revenue minus cost of goods sold)" → "Total gross margin." (NOT "Total gross margin in USD." — neither "USD" nor "$" appears in the description.)
    - Description "Average gross margin per line item" → "Average gross margin per line item." (NOT "Average gross margin per line item in USD.")
    - Description "Weight of the box" → "Weight of the box." (NOT "Weight of the box in lbs." or "Weight of the box in kg.")
    - Description "Total revenue from all line items" → "Total revenue from all line items." (NOT "Total revenue in USD.")
    - Description "Wholesale cost of this inventory item" → "Wholesale cost of an item." (NOT "Wholesale cost in USD.")
    - Description "Order delivery duration" → "Order delivery duration." (NOT "Order delivery duration in days.")
    - Description "Customer lifetime spend in USD" → "Customer lifetime spend in USD." (USD appears verbatim, so keep it.)
  - **Computation / formula tokens are NOT a retrieval signal.** Do NOT include tokens like "sum of", "minus", "divided by", "ratio of", "calculated as", or any restatement of the formula. Computation belongs in the description, not the keyphrase. Describe what the field IS (e.g., "Profit margin per item."), not how it is computed (e.g., NOT "Profit margin, calculated as sale price minus cost.").
* Distill ONLY the parts of the description that help a stranger recognize this field for retrieval. Drop internal process steps, ticket numbers, references like "we use this for X", and other operational noise.
* Drop CRM/marketing/SQL-implementation filler that appeared in the description: "lifecycle", "tenure", "primary key", "lookup table", "behavioral patterns", "across the platform", "within the system".
* Do NOT classify the data or invent its purpose. Forbidden vocabulary unless it appears verbatim in the description, field code, or schema you were given: "personal", "personally identifiable", "PII", "biometric", "biological", "biological sex", "sensitive", "private", "for identification", "for communication", "for tracking", "for analysis", "for reporting", "for storage", "stored within", "stored in the database", "in the database table", "in the customer database", "information source", "data source". If the description only says "Gender: Male or Female", the keyphrase must NOT add "biological sex" or any other classification claim. State only what the description / code / schema asserts.
* Do NOT use padding doublings or stacked quantifiers: "single individual", "individual specific", "specific specific", "the individual user", "each individual record", "particular specific", "Total number of unique", "the total count of all individual". Pick ONE quantifier; drop the redundant one.
* **IMPORTANT: Trivial \`count()\` measures.** If the field is a simple count measure (name like \`user_count\`, \`product_count\`, \`order_count\`, \`inventory_item_count\`, etc.) the keyphrase MUST be a short "Count of \`<plural-noun>\`." form, e.g. "Count of users.", "Count of products.", "Count of orders.". You MUST NOT add scope qualifiers like "currently registered", "currently active", "available within", or "who have placed orders" unless that qualifier is present verbatim in the description / field code. The \`<plural-noun>\` is the entity being counted (users / products / orders / items), NOT the source name.
* **IMPORTANT: Banned source/scope filler patterns.** The keyphrase must NOT name or paraphrase the source, package, or model. In particular, the source name "##SOURCE##" — and any other source name from the schema — must NOT appear in the keyphrase, in any form (literal, lowercased, or as an adjective like "##SOURCE## platform" / "##SOURCE## system" / "##SOURCE## catalog" / "##SOURCE## database" / "##SOURCE## data source"). Also banned are generic scope-filler clauses regardless of source name:
  - "within the system platform"
  - "within the current dataset or filtered view"
  - "in the customer database" / "in the inventory database" / "in the database table"
  - "associated with the registered user account in the system"
  - "for shipping and billing purposes"
  - "stored within … the product catalog"
  If you find yourself appending one of these, drop the trailing scope clause entirely; a short keyphrase that omits scope is better than one that invents or names scope.
* **Trivial common-noun fields.** If the field's name is itself a common English noun (e.g. \`country\`, \`city\`, \`email\`, \`first_name\`, \`last_name\`, \`brand\`, \`age\`, \`latitude\`, \`longitude\`, \`zip\`) and the description is a near-synonym of the name (e.g. "Email address", "Country", "Brand name", "First name"), your keyphrase MUST:
  1. Preserve the literal name token in natural-language form. The keyphrase for \`email\` MUST contain the word "email"; for \`country\` MUST contain "country"; for \`brand\` MUST contain "brand"; etc.
  2. You MAY add ONE short clarifier ONLY if it is grounded in the source / schema / code AND you are confident it is correct (e.g. "email address of the customer", "user's home country", "first name of the registered user", "brand name of the product"). The clarifier should add real retrieval value (the noun the field belongs to: customer / user / product / order).
  3. You MUST NOT invent purpose ("for identification"), classification ("personal", "biometric", "biological"), storage details ("stored within…the database table"), or unstated semantics. If the schema doesn't say it, don't say it.
  4. If you can't think of a confidently grounded clarifier, return the description verbatim or close to it. A short faithful keyphrase is better than a long invented one.
* Do NOT name the source, the model file path, or any file/path. Do NOT use markdown.
* The keyphrase must be one sentence. End with a single period.

`;
const BATCH_TAIL_REPLACEMENT = `Respond only with a JSON array with one object per field, in the following structure, using the index shown in square brackets before each field:

[{"index": <int>, "keyphrase": "<one short distilled retrieval keyphrase>"}]
`;

export interface KeyphraseField {
   source: string;
   name: string;
   /** dimension | measure | view | query | join */
   type: string;
   dataType?: string;
   description: string;
   /** Sibling fields, one per line. */
   schema: string;
   code: string;
   modelPath: string;
}

function fill(template: string, f: KeyphraseField): string {
   return template
      .replaceAll("##SOURCE##", () => f.source)
      .replaceAll("##NAME##", () => f.name)
      .replaceAll("##TYPE##", () => f.type)
      .replaceAll("##DTYPE##", () => f.dataType || "unknown")
      .replaceAll("##DESCRIPTION##", () => f.description)
      .replaceAll("##SCHEMA##", () => f.schema)
      .replaceAll("##CODE##", () => f.code)
      .replaceAll("##MODEL_FILE_PATH##", () => f.modelPath || "unknown");
}

export function buildKeyphrasePrompt(f: KeyphraseField): { system: string; user: string } {
   return { system: KEYPHRASE_ROLE, user: fill(KEYPHRASE_BODY, f) };
}

/** All fields must come from one source. */
export function buildKeyphraseBatchPrompt(fields: KeyphraseField[]): {
   system: string;
   user: string;
} {
   const source = fields[0].source;
   const blocks = fields
      .map(
         (f, i) =>
            `### Field [${i}]\n\nField name: ${f.name}\nField type: ${f.type}\nData type: ${f.dataType || "unknown"}\nDescription: ${f.description}\n\nSibling fields (schema context):\n${f.schema}\n\nField code:\n${f.code}\n\n<END_OF_ENTITY_CODE>`,
      )
      .join("\n\n");
   const user =
      `${BATCH_HEAD}You are given ${fields.length} fields from the Malloy source "${source}". Produce one keyphrase for EACH, applying every rule below to each field independently.\n\n${blocks}` +
      `${BATCH_MID}${BATCH_TAIL_REPLACEMENT}`;
   const model = fields[0].modelPath || "unknown";
   return {
      system: KEYPHRASE_ROLE,
      user: user
         .replaceAll("##SOURCE##", () => source)
         .replaceAll("##MODEL_FILE_PATH##", () => model),
   };
}
