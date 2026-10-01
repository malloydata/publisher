// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** `items.map(fn)` with at most `limit` calls running at once; results keep their order. */
export async function mapWithLimit<T, R>(
   items: readonly T[],
   limit: number,
   fn: (item: T) => Promise<R>,
): Promise<R[]> {
   const out = new Array<R>(items.length);
   let next = 0;
   const worker = async () => {
      while (next < items.length) {
         const i = next++;
         out[i] = await fn(items[i]);
      }
   };
   await Promise.all(
      Array.from(
         { length: Math.max(1, Math.min(limit, items.length)) },
         worker,
      ),
   );
   return out;
}
