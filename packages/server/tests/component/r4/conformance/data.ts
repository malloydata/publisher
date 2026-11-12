// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The fixture's synthetic data: small, deterministic CSVs every shape reads
 * from its package's `data/` directory. No customer data or names: a store
 * with numbered customers, invented regions and generated orders.
 *
 * The rows carry the cases that make a stored answer differ from a live one
 * when something is wrong: a customer with no orders, an order whose customer
 * is missing (and one with none), NULL amounts and regions, two orders on one
 * instant, dates across a year boundary, and a column named by a reserved word.
 */

const REGIONS = ["north", "south", "east", "west"];
const STATUSES = ["placed", "shipped", "returned"];

function csv(header: string[], rows: (string | number | null)[][]): string {
   const cell = (v: string | number | null) =>
      v === null ? "" : typeof v === "number" ? String(v) : `"${v}"`;
   return [header.join(","), ...rows.map((r) => r.map(cell).join(","))].join(
      "\n",
   );
}

/** 8 customers; customer 8 has no orders. */
function customers(): string {
   const rows: (string | number | null)[][] = [];
   for (let id = 1; id <= 8; id++) {
      rows.push([
         id,
         `Customer ${String(id).padStart(2, "0")}`,
         id % 3 === 0 ? "enterprise" : id % 3 === 1 ? "smb" : "consumer",
         REGIONS[id % REGIONS.length],
         `2024-0${(id % 9) + 1}-1${id % 10}`,
      ]);
   }
   return csv(["id", "name", "segment", "home_region", "signup_date"], rows);
}

/** 48 orders over 2024-11 .. 2025-02. */
function orders(): string {
   const rows: (string | number | null)[][] = [];
   for (let id = 1; id <= 48; id++) {
      // Customer 9 does not exist; one order has no customer.
      const customer = id === 47 ? 9 : id === 48 ? null : (id % 7) + 1;
      const region = id % 11 === 0 ? null : REGIONS[(id * 3) % REGIONS.length];
      const amount = id % 13 === 0 ? null : ((id * 37) % 200) + 0.25 * (id % 4);
      const month = ["2024-11", "2024-12", "2025-01", "2025-02"][id % 4];
      const day = String((id % 27) + 1).padStart(2, "0");
      const hour = String((id * 5) % 24).padStart(2, "0");
      // Orders 20 and 21 share an instant.
      const instant =
         id === 21 ? "2025-01-22 05:00:00" : `${month}-${day} ${hour}:15:00`;
      rows.push([
         id,
         customer,
         region,
         STATUSES[id % STATUSES.length],
         amount,
         (id % 5) * 0.05,
         `${month}-${day}`,
         instant,
         (id % 3) + 1,
      ]);
   }
   return csv(
      [
         "id",
         "customer_id",
         "region",
         "status",
         "amount",
         "discount",
         "order_date",
         "created_at",
         "order",
      ],
      rows,
   );
}

/** Two order lines per order, a third on every fifth: the join_many child. */
function lines(): string {
   const rows: (string | number | null)[][] = [];
   let lineId = 1;
   for (let order = 1; order <= 46; order++) {
      const count = order % 5 === 0 ? 3 : order % 9 === 0 ? 0 : 2;
      for (let i = 0; i < count; i++) {
         rows.push([lineId++, order, `sku-${(order + i) % 6}`, (i + 1) * 2]);
      }
   }
   return csv(["line_id", "order_id", "sku", "qty"], rows);
}

/** Every CSV a shape's package carries, by package-relative path. */
export const DATA_FILES: Record<string, string> = {
   "data/customers.csv": customers(),
   "data/orders.csv": orders(),
   "data/lines.csv": lines(),
};

/**
 * The raw sources every shape starts from, as a shared file a shape imports:
 * `orders`, `customers`, `lines`. Explicit types, so a CSV's inference cannot
 * differ between a build and a read.
 */
export const RAW_MODEL = `source: orders_raw is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(customer_id AS BIGINT) AS customer_id,
         region, status,
         CAST(amount AS DOUBLE) AS amount, CAST(discount AS DOUBLE) AS discount,
         CAST(order_date AS DATE) AS order_date,
         CAST(created_at AS TIMESTAMP) AS created_at,
         CAST("order" AS BIGINT) AS "order"
  FROM read_csv('data/orders.csv', header=true, all_varchar=true)
""") extend { primary_key: id }

source: customers_raw is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, name, segment, home_region,
         CAST(signup_date AS DATE) AS signup_date
  FROM read_csv('data/customers.csv', header=true, all_varchar=true)
""") extend { primary_key: id }

source: lines_raw is duckdb.sql("""
  SELECT CAST(line_id AS BIGINT) AS line_id, CAST(order_id AS BIGINT) AS order_id,
         sku, CAST(qty AS BIGINT) AS qty
  FROM read_csv('data/lines.csv', header=true, all_varchar=true)
""") extend { primary_key: line_id }
`;
