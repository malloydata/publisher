// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** The reply was not usable JSON for the caller, even after one re-ask. */
export class LlmJsonError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "LlmJsonError";
   }
}

/**
 * Pull a JSON value out of model text. Models wrap JSON in code fences or add
 * a sentence before it, even when told not to. Tries, in order: the whole
 * text, the body of a code fence, then the span from the first `{` or `[` to
 * the last matching `}` or `]`. Throws with the parse error when none parses.
 */
export function extractJson(text: string): unknown {
   const trimmed = text.trim();
   const candidates: string[] = [trimmed];
   const fence = /```(?:json)?\s*([\s\S]*?)```/i.exec(trimmed);
   if (fence) candidates.push(fence[1].trim());
   const open = trimmed.search(/[{[]/);
   if (open >= 0) {
      const closer = trimmed[open] === "{" ? "}" : "]";
      const close = trimmed.lastIndexOf(closer);
      if (close > open) candidates.push(trimmed.slice(open, close + 1));
   }
   let lastError = "no JSON found in the reply";
   for (const candidate of candidates) {
      try {
         return JSON.parse(candidate);
      } catch (error) {
         lastError = (error as Error).message;
      }
   }
   throw new Error(`the reply is not valid JSON (${lastError})`);
}

/** Parse then validate; the error message is what a repair prompt shows the model. */
export function parseAndValidate<T>(
   text: string,
   validate: (value: unknown) => T,
): T {
   return validate(extractJson(text));
}
