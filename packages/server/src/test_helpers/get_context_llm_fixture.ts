// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A small hand-written package for the LLM-stage specs: plain stand-ins for a
 * compiled package, the same shape get_context_payload_pin.spec.ts uses.
 */

import type { PackageRetrievalSettings } from "../service/package_retrieval";

const field = (kind: string, name: string, doc?: string) => ({
   kind,
   name,
   annotations: doc ? [`#(doc) ${doc}`] : [],
});

const ORDERS_MODEL = {
   getSourceInfos: () => [
      {
         name: "orders",
         annotations: ["#(doc) One row per customer order."],
         schema: {
            fields: [
               field("view", "by_month", "Orders per month."),
               field("dimension", "status", "Order lifecycle status."),
               field("dimension", "state", "State the order ships to."),
               field("dimension", "city", "City the order ships to."),
               field("measure", "total_revenue", "Sum of order revenue."),
               field("measure", "order_count", "Number of orders."),
            ],
         },
      },
   ],
   getQueries: () => [],
};

const CUSTOMERS_MODEL = {
   getSourceInfos: () => [
      {
         name: "customers",
         annotations: ["#(doc) One row per customer."],
         schema: {
            fields: [
               field("dimension", "state", "State the customer lives in."),
               field("dimension", "region", "Sales region of the customer."),
               field("measure", "customer_count", "Distinct customers."),
            ],
         },
      },
   ],
   getQueries: () => [],
};

const SHIPPING_MODEL = {
   getSourceInfos: () => [
      {
         name: "shipments",
         annotations: ["#(doc) One row per shipment leaving a warehouse."],
         schema: {
            fields: [
               field("dimension", "ship_state", "State a shipment goes to."),
               field(
                  "dimension",
                  "carrier",
                  "Carrier that moved the shipment.",
               ),
               field("measure", "shipment_count", "Number of shipments."),
            ],
         },
      },
   ],
   getQueries: () => [],
};

/** Three sources that share vocabulary, so a question can match more than one. */
export function shopPackage(retrieval: Partial<PackageRetrievalSettings> = {}) {
   const models: Record<string, unknown> = {
      "orders.malloy": ORDERS_MODEL,
      "customers.malloy": CUSTOMERS_MODEL,
      "shipping.malloy": SHIPPING_MODEL,
   };
   // Keyphrases and source summaries are off so the index sync never asks the
   // (scripted) chat model anything: these specs count the chat calls
   // get_context itself makes. The source summary specs turn summaries on.
   const settings: PackageRetrievalSettings = {
      representation: "single",
      keyphrases: "never",
      sourceSummary: { enabled: false },
      prompts: {},
      ...retrieval,
   };
   return {
      listModels: async () => Object.keys(models).map((p) => ({ path: p })),
      getModel: (p: string) => models[p],
      getRetrievalSettings: () => settings,
   };
}
