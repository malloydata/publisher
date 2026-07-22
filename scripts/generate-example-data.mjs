// Regenerate the datasets that back the Publisher example packages.
//
//   node scripts/generate-example-data.mjs
//   bun run generate:example-data
//
// Everything here is deterministic (seeded PRNG), so re-running produces the
// exact same files — a diff only appears when this generator changes. Data is
// built in memory, written to temporary JSON, and converted to Parquet (or CSV)
// with the bundled DuckDB bindings. No network, no credentials.
//
// Datasets:
//   examples/storefront/data/{customers,products,order_items}.parquet
//   examples/storefront/data/regions.csv
//   examples/governed-analytics/orders.parquet
//   examples/html-data-app/subscriptions.parquet
import { DuckDBInstance } from "@duckdb/node-api";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

// ── Deterministic PRNG (mulberry32) ────────────────────────────────────────
function makeRng(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rng = makeRng(0x5f0a17); // one stream for the whole run — order matters
const rand = () => rng();
const randInt = (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1));
const pick = (arr) => arr[Math.floor(rand() * arr.length)];
const round2 = (n) => Math.round(n * 100) / 100;

// Weighted pick: items is [[value, weight], ...]
function weighted(items) {
  const total = items.reduce((s, [, w]) => s + w, 0);
  let r = rand() * total;
  for (const [value, w] of items) {
    if ((r -= w) <= 0) return value;
  }
  return items[items.length - 1][0];
}

const iso = (d) => d.toISOString().slice(0, 10);
const addDays = (date, n) => new Date(date.getTime() + n * 86400000);
const monthLabel = (y, m) => `${y}-${String(m + 1).padStart(2, "0")}`;

// ── Reference data ──────────────────────────────────────────────────────────

// (full state name, representative city, relative population weight, US Census region)
const STATES = [
  ["California", "Los Angeles", 39, "West"], ["Texas", "Houston", 30, "South"],
  ["Florida", "Miami", 22, "South"], ["New York", "New York", 20, "Northeast"],
  ["Pennsylvania", "Philadelphia", 13, "Northeast"], ["Illinois", "Chicago", 13, "Midwest"],
  ["Ohio", "Columbus", 12, "Midwest"], ["Georgia", "Atlanta", 11, "South"],
  ["North Carolina", "Charlotte", 11, "South"], ["Michigan", "Detroit", 10, "Midwest"],
  ["New Jersey", "Newark", 9, "Northeast"], ["Virginia", "Virginia Beach", 9, "South"],
  ["Washington", "Seattle", 8, "West"], ["Arizona", "Phoenix", 7, "West"],
  ["Massachusetts", "Boston", 7, "Northeast"], ["Tennessee", "Nashville", 7, "South"],
  ["Indiana", "Indianapolis", 7, "Midwest"], ["Missouri", "Kansas City", 6, "Midwest"],
  ["Maryland", "Baltimore", 6, "South"], ["Wisconsin", "Milwaukee", 6, "Midwest"],
  ["Colorado", "Denver", 6, "West"], ["Minnesota", "Minneapolis", 6, "Midwest"],
  ["South Carolina", "Columbia", 5, "South"], ["Alabama", "Birmingham", 5, "South"],
  ["Louisiana", "New Orleans", 5, "South"], ["Kentucky", "Louisville", 4, "South"],
  ["Oregon", "Portland", 4, "West"], ["Oklahoma", "Oklahoma City", 4, "South"],
  ["Connecticut", "Hartford", 4, "Northeast"], ["Utah", "Salt Lake City", 3, "West"],
  ["Iowa", "Des Moines", 3, "Midwest"], ["Nevada", "Las Vegas", 3, "West"],
  ["Arkansas", "Little Rock", 3, "South"], ["Mississippi", "Jackson", 3, "South"],
  ["Kansas", "Wichita", 3, "Midwest"], ["New Mexico", "Albuquerque", 2, "West"],
  ["Nebraska", "Omaha", 2, "Midwest"], ["Idaho", "Boise", 2, "West"],
  ["West Virginia", "Charleston", 2, "South"], ["Hawaii", "Honolulu", 1, "West"],
  ["New Hampshire", "Manchester", 1, "Northeast"], ["Maine", "Portland", 1, "Northeast"],
  ["Montana", "Billings", 1, "West"], ["Rhode Island", "Providence", 1, "Northeast"],
  ["Delaware", "Wilmington", 1, "South"], ["South Dakota", "Sioux Falls", 1, "Midwest"],
  ["North Dakota", "Fargo", 1, "Midwest"], ["Alaska", "Anchorage", 1, "West"],
  ["Vermont", "Burlington", 1, "Northeast"], ["Wyoming", "Cheyenne", 1, "West"],
];

const FIRST_NAMES = [
  "Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "Elijah", "Sophia",
  "Mateo", "Isabella", "Lucas", "Mia", "Levi", "Amelia", "Ezra", "Harper",
  "Asher", "Evelyn", "Leo", "Luna", "Ethan", "Camila", "Kai", "Aria",
  "Jack", "Chloe", "Aiden", "Layla", "Grayson", "Riley", "Julian", "Nora",
  "Wyatt", "Zoe", "Owen", "Mila", "Caleb", "Aurora", "Nathan", "Ellie",
  "Hudson", "Maya", "Isaiah", "Naomi", "Miles", "Delilah", "Theo", "Sofia",
];
const LAST_NAMES = [
  "Kim", "Brooks", "Nguyen", "Patel", "Garcia", "Rivera", "Chen", "Okafor",
  "Rossi", "Hassan", "Silva", "Nakamura", "Andersson", "Cohen", "Murphy",
  "Reyes", "Ali", "Wagner", "Santos", "Petrov", "Larsen", "Diaz", "Yamada",
  "Novak", "Haddad", "Costa", "Fischer", "Moreau", "Ivanov", "Park",
  "Bianchi", "Kowalski", "Mensah", "Tanaka", "Sharma", "Dubois", "Vega",
];

// (adjective pool, noun) per category — product names read like a catalog
const CATEGORIES = [
  ["Jeans", ["Slim", "Relaxed", "Straight", "Tapered", "Bootcut"], ["Jean", "Denim"]],
  ["Tops", ["Everyday", "Boxy", "Ribbed", "Draped", "Classic"], ["Tee", "Top", "Blouse"]],
  ["Outerwear", ["Quilted", "Packable", "Insulated", "Waxed", "Hooded"], ["Jacket", "Parka", "Coat"]],
  ["Dresses", ["Wrap", "Tiered", "Midi", "Slip", "Shirt"], ["Dress"]],
  ["Footwear", ["Trail", "Court", "Chelsea", "Canvas", "Runner"], ["Sneaker", "Boot", "Loafer"]],
  ["Accessories", ["Woven", "Leather", "Canvas", "Knit", "Structured"], ["Belt", "Tote", "Scarf", "Cap"]],
  ["Activewear", ["Featherweight", "Compression", "Seamless", "Training", "Studio"], ["Legging", "Short", "Tank"]],
  ["Sweaters", ["Merino", "Chunky", "Cashmere", "Waffle", "Crew"], ["Sweater", "Cardigan", "Pullover"]],
  ["Shorts", ["Pleated", "Cargo", "Chino", "Linen", "Utility"], ["Short"]],
  ["Swimwear", ["Ribbed", "Sculpt", "Colorblock", "Board", "Retro"], ["Swimsuit", "Trunk"]],
];
const BRANDS = [
  "Summit", "Cobalt", "Meridian", "Harbor", "Alpine", "Vertex", "Willow",
  "Nomad", "Aurora", "Drift", "Cedar", "Loft",
];

const ORDER_STATUSES = [
  ["Complete", 68], ["Shipped", 14], ["Processing", 8], ["Returned", 6], ["Cancelled", 4],
];

// Codepoint order, not localeCompare: the row order reaches the CSV bytes
// verbatim, and collation for "New York" / "Wisconsin" varies by locale.
function buildRegions() {
  return STATES.map(([state, , , region]) => ({ state, region })).sort((a, b) =>
    a.state < b.state ? -1 : a.state > b.state ? 1 : 0,
  );
}

// ── Storefront ──────────────────────────────────────────────────────────────
function buildStorefront() {
  const N_CUSTOMERS = 1000;
  const N_PRODUCTS = 200;

  // Customers, weighted across states; signup over ~4 years.
  const customers = [];
  const signupStart = new Date("2022-01-01");
  for (let i = 1; i <= N_CUSTOMERS; i++) {
    const [state, city] = weighted(STATES.map((s) => [s, s[2]]));
    customers.push({
      customer_id: i,
      full_name: `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)}`,
      state,
      city,
      signup_date: iso(addDays(signupStart, randInt(0, 365 * 4 - 1))),
    });
  }
  // A per-customer propensity so a minority become frequent repeat buyers.
  const propensity = customers.map(() => 0.2 + rand() * rand() * 4);
  const propTotal = propensity.reduce((a, b) => a + b, 0);

  // Products with cost/retail by category price band.
  const priceBands = {
    Jeans: [30, 120], Tops: [18, 70], Outerwear: [80, 320], Dresses: [45, 180],
    Footwear: [45, 190], Accessories: [15, 95], Activewear: [25, 90],
    Sweaters: [40, 160], Shorts: [22, 75], Swimwear: [28, 110],
  };
  const products = [];
  for (let i = 1; i <= N_PRODUCTS; i++) {
    const [category, adjs, nouns] = pick(CATEGORIES);
    const brand = pick(BRANDS);
    const [lo, hi] = priceBands[category];
    const retail = round2(lo + rand() * (hi - lo));
    const cost = round2(retail * (0.38 + rand() * 0.22)); // 38–60% of retail
    products.push({
      product_id: i,
      name: `${brand} ${pick(adjs)} ${pick(nouns)}`,
      category,
      brand,
      cost,
      retail_price: retail,
    });
  }

  // Orders/lines over 36 months with growth + holiday seasonality.
  const MONTHS = [];
  for (let y = 2023; y <= 2025; y++)
    for (let m = 0; m < 12; m++) MONTHS.push([y, m]);
  const seasonal = [0.9, 0.85, 0.95, 1.0, 1.02, 1.0, 0.98, 1.02, 1.05, 1.1, 1.45, 1.6];
  const monthWeights = MONTHS.map(([, m], idx) => Math.pow(1.025, idx) * seasonal[m]);
  const wTotal = monthWeights.reduce((a, b) => a + b, 0);

  const TARGET_ORDERS = 11000;
  const order_items = [];
  let orderId = 5001;
  let lineId = 100001;

  const chooseCustomer = () => {
    let r = rand() * propTotal;
    for (let i = 0; i < propensity.length; i++)
      if ((r -= propensity[i]) <= 0) return i;
    return propensity.length - 1;
  };

  MONTHS.forEach(([y, m], idx) => {
    const monthOrders = Math.round((TARGET_ORDERS * monthWeights[idx]) / wTotal);
    const daysInMonth = new Date(y, m + 1, 0).getDate();
    const recent = idx >= MONTHS.length - 2; // last two months skew "in flight"
    for (let o = 0; o < monthOrders; o++) {
      const custIdx = chooseCustomer();
      const created = iso(new Date(y, m, randInt(1, daysInMonth)));
      const nLines = weighted([[1, 34], [2, 30], [3, 18], [4, 10], [5, 5], [6, 3]]);
      const status = recent
        ? weighted([["Processing", 34], ["Shipped", 30], ["Complete", 30], ["Cancelled", 6]])
        : weighted(ORDER_STATUSES);
      for (let l = 0; l < nLines; l++) {
        const p = products[randInt(0, products.length - 1)];
        const discount = weighted([[0, 68], [0.1, 16], [0.2, 10], [0.3, 6]]);
        order_items.push({
          id: lineId++,
          order_id: orderId,
          customer_id: customers[custIdx].customer_id,
          product_id: p.product_id,
          sale_price: round2(p.retail_price * (1 - discount)),
          status,
          created_at: created,
        });
      }
      orderId++;
    }
  });

  return { customers, products, order_items };
}

// ── Governed analytics ───────────────────────────────────────────────────────
function buildGoverned() {
  const regions = ["us-east", "us-west", "emea"];
  const tenants = ["acme", "globex", "initech"];
  const statuses = [["Complete", 62], ["Pending", 22], ["Returned", 16]];
  const rows = [];
  let orderId = 1001;
  // 24 months, gentle upward trend.
  const MONTHS = [];
  for (let y = 2024; y <= 2025; y++)
    for (let m = 0; m < 12; m++) MONTHS.push([y, m]);
  MONTHS.forEach(([y, m], idx) => {
    const monthOrders = Math.round(160 * Math.pow(1.02, idx));
    const daysInMonth = new Date(y, m + 1, 0).getDate();
    for (let o = 0; o < monthOrders; o++) {
      rows.push({
        order_id: orderId++,
        region: pick(regions),
        tenant: pick(tenants),
        status: weighted(statuses),
        amount: round2(50 + rand() * 949),
        order_date: iso(new Date(y, m, randInt(1, daysInMonth))),
      });
    }
  });
  return rows;
}

// ── SaaS subscriptions (html-data-app) ───────────────────────────────────────
function buildSubscriptions() {
  const PLANS = [
    ["Starter", [39, 99], [1, 5], 30],
    ["Pro", [199, 399], [5, 25], 34],
    ["Business", [599, 1200], [20, 80], 24],
    ["Enterprise", [1800, 6000], [75, 400], 12],
  ];
  const INDUSTRIES = [
    "Software", "Financial Services", "Healthcare", "Retail", "Education",
    "Media", "Manufacturing", "Nonprofit",
  ];
  const COUNTRIES = [
    ["United States", 44], ["Canada", 10], ["United Kingdom", 10],
    ["Germany", 8], ["France", 6], ["Australia", 6], ["India", 6],
    ["Brazil", 5], ["Netherlands", 5],
  ];
  const PREFIX = [
    "North", "Blue", "Bright", "Data", "Cloud", "Peak", "Nova", "Iron",
    "Green", "Swift", "Prime", "Vela", "Echo", "Lumen", "Atlas", "Orbit",
    "Cedar", "Delta", "Onyx", "Quill",
  ];
  const SUFFIX = [
    "Labs", "Systems", "Group", "Works", "Analytics", "Digital", "Partners",
    "Technologies", "Health", "Retail", "Studio", "Networks", "Dynamics",
    "Collective", "Industries",
  ];

  const rows = [];
  const N = 800;
  const startWindow = new Date("2023-01-01");
  for (let i = 1; i <= N; i++) {
    const [plan, [mrrLo, mrrHi], [seatLo, seatHi]] = weighted(
      PLANS.map((p) => [p, p[3]]),
    );
    const started = addDays(startWindow, randInt(0, 365 * 3 - 1));
    // Churn probability decays for higher tiers; trials are recent-only.
    const churnP = { Starter: 0.28, Pro: 0.18, Business: 0.11, Enterprise: 0.05 }[plan];
    const daysSince = Math.floor((new Date("2025-12-31") - started) / 86400000);
    let status, churned_at = null;
    if (daysSince < 45 && rand() < 0.5) {
      status = "Trial";
    } else if (rand() < churnP) {
      status = "Churned";
      churned_at = iso(addDays(started, randInt(60, Math.max(90, daysSince))));
    } else {
      status = "Active";
    }
    rows.push({
      subscription_id: i,
      account_name: `${pick(PREFIX)} ${pick(SUFFIX)}`,
      plan,
      industry: pick(INDUSTRIES),
      country: weighted(COUNTRIES),
      seats: randInt(seatLo, seatHi),
      mrr: round2(mrrLo + rand() * (mrrHi - mrrLo)),
      status,
      started_at: iso(started),
      churned_at,
    });
  }
  return rows;
}

// ── Write helpers ────────────────────────────────────────────────────────────
async function writeTable(con, tmp, outPath, rows, columns, format) {
  const base = outPath.replace(/[\/]/g, "_");
  const jsonPath = join(tmp, `${base}.json`);
  await writeFile(jsonPath, JSON.stringify(rows));
  const colSpec = Object.entries(columns)
    .map(([c, t]) => `'${c}': '${t}'`)
    .join(", ");
  await con.run(
    `COPY (SELECT * FROM read_json('${jsonPath.replace(/'/g, "''")}', ` +
      `format='array', columns={${colSpec}})) ` +
      `TO '${outPath}' (FORMAT ${format})`,
  );
}

const writeParquet = (con, tmp, outPath, rows, columns) =>
  writeTable(con, tmp, outPath, rows, columns, "PARQUET");

const writeCsv = (con, tmp, outPath, rows, columns) =>
  writeTable(con, tmp, outPath, rows, columns, "CSV, HEADER");

async function main() {
  const inst = await DuckDBInstance.create(":memory:");
  const con = await inst.connect();
  const tmp = await mkdtemp(join(tmpdir(), "publisher-data-"));

  try {
    const sf = buildStorefront();
    await writeParquet(con, tmp, "examples/storefront/data/customers.parquet", sf.customers, {
      customer_id: "BIGINT", full_name: "VARCHAR", state: "VARCHAR",
      city: "VARCHAR", signup_date: "DATE",
    });
    await writeParquet(con, tmp, "examples/storefront/data/products.parquet", sf.products, {
      product_id: "BIGINT", name: "VARCHAR", category: "VARCHAR",
      brand: "VARCHAR", cost: "DOUBLE", retail_price: "DOUBLE",
    });
    await writeParquet(con, tmp, "examples/storefront/data/order_items.parquet", sf.order_items, {
      id: "BIGINT", order_id: "BIGINT", customer_id: "BIGINT",
      product_id: "BIGINT", sale_price: "DOUBLE", status: "VARCHAR",
      created_at: "DATE",
    });

    // CSV on purpose: a small lookup a human would keep in a spreadsheet, and a
    // worked example that duckdb.table() reads CSV and Parquet in one model.
    const regions = buildRegions();
    await writeCsv(con, tmp, "examples/storefront/data/regions.csv", regions, {
      state: "VARCHAR", region: "VARCHAR",
    });

    const gov = buildGoverned();
    await writeParquet(con, tmp, "examples/governed-analytics/orders.parquet", gov, {
      order_id: "BIGINT", region: "VARCHAR", tenant: "VARCHAR",
      status: "VARCHAR", amount: "DOUBLE", order_date: "DATE",
    });

    const subs = buildSubscriptions();
    await writeParquet(con, tmp, "examples/html-data-app/subscriptions.parquet", subs, {
      subscription_id: "BIGINT", account_name: "VARCHAR", plan: "VARCHAR",
      industry: "VARCHAR", country: "VARCHAR", seats: "BIGINT",
      mrr: "DOUBLE", status: "VARCHAR", started_at: "DATE", churned_at: "DATE",
    });

    console.log("Wrote:");
    console.log(`  storefront   customers=${sf.customers.length} products=${sf.products.length} order_items=${sf.order_items.length} regions=${regions.length} (csv)`);
    console.log(`  governed     orders=${gov.length}`);
    console.log(`  subscriptions rows=${subs.length}`);
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

await main();
