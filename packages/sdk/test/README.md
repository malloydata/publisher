# Testing React components and hooks in the SDK

`bun test` in this package runs with a DOM, so components and hooks can be
rendered and asserted on. Before this existed every SDK spec was a pure
function, and anything touching React was untestable.

Run them the usual way:

```bash
cd packages/sdk && bun test                              # whole package
cd packages/sdk && bun test src/components/given         # one directory
cd packages/sdk && bun test -t "explicit false"          # one test by name
```

Run them from this package, not from the repo root. Bun reads `bunfig.toml`
from the working directory, so a spec invoked from the root gets no preload and
fails with `ReferenceError: document is not defined`. `bun run test:sdk` at the
root is fine; it changes directory first, and so does CI.

## How the DOM gets there

`bunfig.toml` preloads `test/setup.ts`, which registers happy-dom onto
`globalThis` and installs Testing Library's `cleanup` in an `afterEach`. There
is no per-file setup: write a spec and it has a document.

Two details in that file are load-bearing. Testing Library is imported
dynamically, after the registrator runs, because it binds to `document` at
module scope and a static import would hoist above the line that creates one.
And the setup file lives outside `src/` because `vite-plugin-dts` generates
declarations for everything under `src/`, so a setup file in there would ship a
stray `.d.ts` in `dist/`.

happy-dom rather than jsdom because `@happy-dom/global-registrator` is designed
to install into an existing runtime, which is exactly what a Bun preload can do.
jsdom expects to own the test environment and offers no equivalent hook, so
using it would have meant bringing in a second test runner alongside `bun test`.

## Testing a component

```tsx
import { describe, expect, it, mock } from "bun:test";
import { fireEvent, render, screen } from "@testing-library/react";
import type { GivenValue } from "../../hooks/givenValue";
import { GivenInput } from "./GivenInput";

it("reverts rather than blanking the field", () => {
   const onChange = mock((_next: GivenValue) => {});
   render(<GivenInput given={{ name: "region" }} value="CA" onChange={onChange} />);

   fireEvent.click(screen.getByLabelText("clear value"));

   expect(onChange.mock.calls[0][0]).toBeNull();
});
```

Type the mock's parameter. `mock(() => {})` is a zero-argument mock, so
`mock.calls` is an array of empty tuples: reading `[0][0]` does not compile,
and nothing constrains what the component actually passes you.

Query by what a user can perceive (`getByLabelText`, `getByRole`,
`getByText`) rather than by class name or component internals, so a test
survives a restyle. Use `queryAllBy*` when asserting something is absent;
`getBy*` throws instead of returning empty.

`src/components/given/GivenInput.spec.tsx` is the worked example.

## Testing a hook

No JSX is needed, so a hook spec is a plain `.spec.ts`.

```ts
import { act, renderHook } from "@testing-library/react";

const { result, rerender } = renderHook(
   ({ types }) => useGivensState({ declaredTypes: types, autorun: true }),
   { initialProps: { types: new Map([["REGION", "filter<string>"]]) } },
);

act(() => result.current.setGiven("REGION", "West"));
expect(result.current.applied.get("REGION")).toBe("West");

rerender({ types: new Map() });   // props change, effects flush
```

`result.current` is re-read after every update, so hold onto the object rather
than destructuring off it. Wrap anything that sets state in `act()`.

`src/hooks/useGivensState.spec.ts` is the worked example, and two things in it
are worth copying. The test named "settles when its inputs are fresh objects on
every render" passes deliberately unmemoized arguments to prove the hook settles
instead of re-rendering forever; that is a real defect class here, and it is
invisible to a test that only ever passes stable inputs. Named rather than
pointed at by position, because "its last test" stopped being that one the first
time a test was appended. And its `renderWithUrlHost` helper feeds the hook's own output back in
as props, the way the page hosts do through the URL. Several defects in that
hook only appear on the second half of that loop, so a spec that treats the
props as constant cannot see them.

## Where specs live, and what not to break

Co-locate a spec with what it tests, matching the existing `*.spec.ts` files.

`tsconfig.json` excludes test files, because that config drives the declaration
build and anything left in it emits a stray `.d.ts` into `dist/`, which ships.
`bun test` discovers four name patterns, and both configs list all four with
the extension left open, so `.tsx`/`.mts`/`.cts` cannot slip past:

```
*.spec.*   *.test.*   *_spec.*   *_test.*
```

Excluded is not unchecked. `tsconfig.spec.json` includes the same four plus
everything in `test/`, and the root `typecheck:sdk` runs it, so a type error in
a spec fails the gate. Without it there would be no gate at all: `bun test`
strips types without checking them, and eslint in this package is not
type-aware.

Keep the two lists in step, and if you add a pattern, put it in both and check
`dist/` afterwards.

## One thing happy-dom will let you get wrong

happy-dom does not implement the HTML value-sanitization algorithm. Set
`"12abc"` on a raw `<input type="number">` and happy-dom keeps it, where a real
browser discards it and leaves the field empty (checked against Chromium). So a
test that hands a number input a non-numeric value and asserts on what it
displays can pass here and be false in production.

Note where that does *not* bite: it only fires if your code passes the bad
value through to the DOM. `GivenInput` substitutes `""` first, so both engines
agree there, and its non-numeric test asserts on the revert affordance rather
than the displayed value.

The general form of that trap: happy-dom is a fast approximation, not a
browser. Behaviour that depends on layout, real CSS, or the finer points of
form-control semantics belongs in the Playwright suite in `packages/app`.
