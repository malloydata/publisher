// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A call refused because the provider failed repeatedly a moment ago. */
export class ProviderCooldownError extends Error {
   constructor(
      readonly provider: string,
      readonly retryInMs: number,
   ) {
      super(
         `${provider} is cooling down after repeated failures; retrying in ${Math.ceil(retryInMs / 1000)}s`,
      );
      this.name = "ProviderCooldownError";
   }
}

/**
 * After `threshold` failed calls in a row, refuse further calls for
 * `windowMs` so a down or misconfigured endpoint costs a few timeouts, not
 * one per entity. A success resets the count. The same idea as the
 * per-package cooldown the embedding index keeps, for chat calls.
 */
export class FailureCooldown {
   private consecutive = 0;
   private until = 0;

   constructor(
      private readonly provider: string,
      private readonly threshold = 3,
      private readonly windowMs = 60_000,
      private readonly now: () => number = Date.now,
   ) {}

   /** Throws {@link ProviderCooldownError} while the window is open. */
   check(): void {
      const left = this.until - this.now();
      if (left > 0) throw new ProviderCooldownError(this.provider, left);
   }

   success(): void {
      this.consecutive = 0;
   }

   failure(): void {
      this.consecutive++;
      if (this.consecutive >= this.threshold) {
         this.until = this.now() + this.windowMs;
         this.consecutive = 0;
      }
   }
}
