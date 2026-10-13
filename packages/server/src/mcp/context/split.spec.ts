// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { phraseCheckerOf, splitBundle } from "./split";

describe("splitBundle", () => {
   it("carries a trailing 'of the ...' subject onto every item", () => {
      expect(splitBundle("state, city and zip of the venue")).toEqual([
         "state of the venue",
         "city of the venue",
         "zip of the venue",
      ]);
   });

   it("keeps an 'or' inside the shared subject", () => {
      expect(
         splitBundle(
            "product name, vendor name, vendor domain, product status, vendor status of the researched or compared product",
         ),
      ).toEqual([
         "product name of the researched or compared product",
         "vendor name of the researched or compared product",
         "vendor domain of the researched or compared product",
         "product status of the researched or compared product",
         "vendor status of the researched or compared product",
      ]);
   });

   it("carries the first item's leading word when most items are one word", () => {
      expect(
         splitBundle("venue state, city, address, zip code or market"),
      ).toEqual([
         "venue state",
         "venue city",
         "venue address",
         "venue zip code",
         "venue market",
      ]);
      expect(
         splitBundle(
            "vendor company headquarters, founded year, employees, funding",
         ),
      ).toEqual([
         "vendor company headquarters",
         "vendor founded year",
         "vendor employees",
         "vendor funding",
      ]);
   });

   it("does not carry a leading word onto items that name their own head", () => {
      expect(
         splitBundle("product category, traffic source, customer country"),
      ).toEqual(["product category", "traffic source", "customer country"]);
   });

   it("carries a colon prefix, dropping filler words", () => {
      expect(
         splitBundle(
            "guest personal details: name, phone number, birth year or age, gender, email, zip code",
         ),
      ).toEqual([
         "guest personal details name",
         "guest personal details phone number",
         "guest personal details birth year",
         "guest personal details age",
         "guest personal details gender",
         "guest personal details email",
         "guest personal details zip code",
      ]);
      expect(
         splitBundle(
            "anything about customer location: city, state or province, country, postal code",
         ),
      ).toEqual([
         "customer location city",
         "customer location state",
         "customer location province",
         "customer location country",
         "customer location postal code",
      ]);
   });

   it("does not split a phrase with no comma, semicolon or slashed list", () => {
      for (const t of [
         "first and last name",
         "profit and loss",
         "shipping and handling",
         "buy/sell",
         "year-over-year revenue growth",
         "anything about the card used for the payment",
         "research and development spend",
      ]) {
         expect(splitBundle(t)).toEqual([]);
      }
   });

   it("keeps a modifier on the item before it", () => {
      expect(splitBundle("price per unit, after discount")).toEqual([]);
      expect(splitBundle("net sales, after returns, order count")).toEqual([
         "net sales, after returns",
         "order count",
      ]);
   });

   it("never cuts an example tail or a parenthesis", () => {
      expect(
         splitBundle(
            "venue category or archetype such as cocktail bar, dive bar, sports bar",
         ),
      ).toEqual([]);
      expect(splitBundle("item category (e.g. Cocktails, Beer, Wine)")).toEqual(
         [],
      );
   });

   it("shares a head noun across a short 'and' list", () => {
      expect(splitBundle("first and last name, email")).toEqual([
         "first name",
         "last name",
         "email",
      ]);
   });

   it("keeps a phrase the package uses whole", () => {
      const known = phraseCheckerOf([
         "shipping_and_handling",
         "Fee for shipping and handling, in USD.",
      ]);
      expect(splitBundle("total, shipping and handling, tax", known)).toEqual([
         "total",
         "shipping and handling",
         "tax",
      ]);
      expect(splitBundle("total, shipping and handling, tax")).toEqual([
         "total",
         "shipping",
         "handling",
         "tax",
      ]);
   });

   it("reads three spaced slashes as a list", () => {
      expect(splitBundle("daypart / hour of day / day of week")).toEqual([
         "daypart",
         "hour of day",
         "day of week",
      ]);
   });

   it("caps the items", () => {
      expect(
         splitBundle("a1, b2, c3, d4, e5, f6, g7, h8, i9, j10"),
      ).toHaveLength(8);
   });

   it("keeps 'by ...' as an item, and carries no adjective or long first item", () => {
      expect(
         splitBundle(
            "year-over-year comparison, new vs returning customers, by territory",
         ),
      ).toEqual([
         "year-over-year comparison",
         "new vs returning customers",
         "by territory",
      ]);
      expect(
         splitBundle(
            "net revenue after discounts and refunds, order count, AOV",
         ),
      ).toEqual([
         "net revenue after discounts and refunds",
         "order count",
         "AOV",
      ]);
      expect(splitBundle("net revenue, refunds, returns")).toEqual([
         "net revenue",
         "refunds",
         "returns",
      ]);
   });

   it("never splits a question or a sentence", () => {
      for (const t of [
         "Which venue had the highest total check revenue in August 2026, and how much was it?",
         "Across July to September 2026, what share of alcoholic item lines were cocktails",
         "For credit transactions, what would be the average fee that the card scheme GlobalCard would charge",
         "Between 2026-09-05 and 2026-09-14 inclusive, how many real agent turns used Claude Haiku",
         "the number of orders that were opened across every venue in the network, voided orders included",
      ]) {
         expect(splitBundle(t)).toEqual([]);
      }
   });

   it("keeps a qualifier or example values on the field they describe", () => {
      for (const t of [
         "kind of account a merchant holds, retail or SaaS",
         "customer region, including no region",
         "company size segment, all sizes",
         "real users, not test or internal",
         "real traffic, not synthetic",
      ]) {
         expect(splitBundle(t)).toEqual([]);
      }
   });
});
