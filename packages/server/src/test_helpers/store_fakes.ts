// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The members a spec stubs on one of the store's narrow roles
 * (service/environment_ports.ts). Member names and argument shapes are
 * checked against the role; a method may return a partial stand-in (an
 * environment with only the methods the code under test calls).
 */
export type StoreStub<T> = {
   [K in keyof T]?: T[K] extends (...args: infer A) => unknown
      ? (...args: A) => unknown
      : T[K];
};

/**
 * A fake of one narrow store role, holding only the members given. The rest
 * are absent, so a call the spec did not expect fails as it would on any
 * partial stub.
 */
export function fakeOf<T extends object>(members: StoreStub<T>): T {
   return members as T;
}
