// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An in-process BM25 keyword index over the catalog's search texts. It ranks
 * beside embedding closeness in the find stage and the two are fused by rank,
 * because each finds what the other misses: closeness finds "revenue" for
 * "sales", keywords find `cust_id` for "customer id" and a rare word a
 * vector smears. Rebuilt in memory when the catalog is; nothing is stored.
 *
 * Names are split the way a person reads them (`cust_id` is "cust id",
 * `totalSales` is "total sales"), words are lowercased and reduced by a
 * light stemmer, and common English words are dropped.
 */

const STOPWORDS = new Set(
   (
      "a an and are as at be by for from has have how i in is it its many much of on or per " +
      "that the their this to was what when where which who why with each all any do does " +
      "me my show give get list tell find number total count"
   ).split(" "),
);

/** Reduce a word to a stem: enough to make "orders" meet "order" and "shipped" meet "ship". */
export function stem(word: string): string {
   let w = word;
   if (w.length > 4 && w.endsWith("ies")) w = `${w.slice(0, -3)}y`;
   else if (w.length > 4 && w.endsWith("sses")) w = w.slice(0, -2);
   else if (
      w.length > 3 &&
      w.endsWith("s") &&
      !w.endsWith("ss") &&
      !w.endsWith("us")
   )
      w = w.slice(0, -1);
   let cut = false;
   if (w.length > 5 && w.endsWith("ing")) {
      w = w.slice(0, -3);
      cut = true;
   } else if (w.length > 4 && w.endsWith("ed")) {
      w = w.slice(0, -2);
      cut = true;
   }
   // "shipped" -> "shipp" -> "ship"; "running" -> "runn" -> "run".
   if (
      cut &&
      w.length > 3 &&
      /([b-df-hj-np-tv-z])\1$/.test(w) &&
      !w.endsWith("ll") &&
      !w.endsWith("ss")
   ) {
      w = w.slice(0, -1);
   }
   return w;
}

/** The terms of a text: split, lowercased, stopwords dropped, stemmed. */
export function terms(text: string): string[] {
   return text
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((w) => w.length > 0 && !STOPWORDS.has(w))
      .map(stem);
}

export interface KeywordHit {
   key: string;
   score: number;
}

const K1 = 1.2;
const B = 0.75;

export class KeywordIndex {
   private readonly docs: Array<{
      key: string;
      tf: Map<string, number>;
      len: number;
   }> = [];
   private readonly df = new Map<string, number>();
   private avgLen = 0;

   constructor(entries: Iterable<{ key: string; text: string }>) {
      let total = 0;
      for (const { key, text } of entries) {
         const tf = new Map<string, number>();
         const ts = terms(text);
         for (const t of ts) tf.set(t, (tf.get(t) ?? 0) + 1);
         for (const t of tf.keys()) this.df.set(t, (this.df.get(t) ?? 0) + 1);
         this.docs.push({ key, tf, len: ts.length });
         total += ts.length;
      }
      this.avgLen = this.docs.length > 0 ? total / this.docs.length : 0;
   }

   get size(): number {
      return this.docs.length;
   }

   /**
    * BM25 scores of the documents `allow` admits, best first, ties by key.
    * A document with no query term is left out.
    */
   search(query: string, allow: (key: string) => boolean): KeywordHit[] {
      const q = [...new Set(terms(query))];
      if (q.length === 0) return [];
      const n = this.docs.length;
      const idf = new Map(
         q.map((t) => {
            const df = this.df.get(t) ?? 0;
            return [t, Math.log(1 + (n - df + 0.5) / (df + 0.5))];
         }),
      );
      const hits: KeywordHit[] = [];
      for (const doc of this.docs) {
         if (!allow(doc.key)) continue;
         let score = 0;
         for (const t of q) {
            const f = doc.tf.get(t);
            if (!f) continue;
            const norm =
               1 - B + B * (this.avgLen > 0 ? doc.len / this.avgLen : 1);
            score += (idf.get(t) ?? 0) * ((f * (K1 + 1)) / (f + K1 * norm));
         }
         if (score > 0) hits.push({ key: doc.key, score });
      }
      return hits.sort(
         (a, b) =>
            b.score - a.score || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0),
      );
   }
}
