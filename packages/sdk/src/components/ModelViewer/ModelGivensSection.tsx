// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { useCallback, useMemo, useState } from "react";
import type { GivenValue } from "../../hooks/givenValue";
import { useSettled } from "../../hooks/useSettled";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { GivensPanel } from "../given/GivensPanel";
import type { GivenInfo } from "./modelGivens";
import { ModelSection } from "./ModelSection";
import { BodyText } from "./text";

/** How long given values hold still before the suggest queries behind a select re-run. */
const GIVEN_SETTLE_MS = 400;

/**
 * The page's Givens section: the givens this source reads, as a page section
 * like its Views and fields (title, "2 givens"), first on the page because they
 * set every run below it. Inside it, the SDK's `GivensPanel` in its `bar`
 * layout, the dashboard's filter bar, so a given is set with the same control,
 * the same "Default: …" line and the same Reset on a model page as on a
 * dashboard. None when the source reads no given.
 */
export function ModelGivensSection({
   givens,
   inheritedFrom,
   values,
   onChange,
   onReset,
   environmentName,
   packageName,
   modelPath,
   versionId,
   declaredTypes,
}: {
   /** The givens this source reads (host-filled ones already left out). */
   givens: ReadonlyArray<GivenInfo>;
   /** Given name → the sources it is read by that the page can't open (the
    *  model doesn't export them), for the line naming where it comes from. */
   inheritedFrom?: ReadonlyMap<string, string[]>;
   values: Map<string, GivenValue>;
   onChange: (name: string, value: GivenValue) => void;
   onReset: () => void;
   environmentName: string;
   packageName: string;
   modelPath: string;
   versionId?: string;
   declaredTypes: ReadonlyMap<string, string | undefined>;
}) {
   const specs = useMemo(() => [...givens], [givens]);
   // A picker's options are read when it is first opened, never on render:
   // nothing on the page runs until the reader asks, and a suggest is a query
   // like any other. Once opened, its list stays live for the page.
   const [opened, setOpened] = useState<ReadonlySet<string>>(() => new Set());
   const onControlOpen = useCallback(
      (name: string) =>
         setOpened((current) =>
            current.has(name) ? current : new Set([...current, name]),
         ),
      [],
   );
   // A suggest query carries the values of the givens that gate its source, so
   // it re-runs as they change: settled first, so typing a gating given queries
   // once rather than once per keystroke.
   const settledValues = useSettled(values, GIVEN_SETTLE_MS);
   const applied = useMemo(
      () => ({ values: settledValues, declaredTypes }),
      [settledValues, declaredTypes],
   );
   const { options, isLoading, failed, truncated, search } = useSuggestOptions(
      environmentName,
      packageName,
      modelPath,
      specs,
      versionId,
      applied,
      undefined,
      opened,
   );
   return (
      <ModelSection
         label="Givens"
         count={givens.length}
         testId="model-givens-section"
      >
         {/* The bar draws no heading of its own; the section's is the page's. */}
         <Box sx={{ "& .MuiPaper-root": { borderColor: "divider" } }}>
            <GivensPanel
               layout="bar"
               givens={specs}
               values={values}
               onChange={onChange}
               onReset={onReset}
               options={options}
               optionsLoading={isLoading}
               optionsFailed={failed}
               optionsTruncated={truncated}
               optionSearch={search}
               onControlOpen={onControlOpen}
            />
         </Box>
         {inheritedFrom && inheritedFrom.size > 0 && (
            <BodyText data-testid="model-givens-inherited" sx={{ mt: 1 }}>
               {[...inheritedFrom]
                  .map(([name, from]) =>
                     from.length === 1
                        ? `${name} is inherited from ${from[0]}, which this model doesn't export.`
                        : `${name} is inherited from ${from.slice(0, -1).join(", ")} and ${from.at(-1)}, none of which this model exports.`,
                  )
                  .join(" ")}
            </BodyText>
         )}
      </ModelSection>
   );
}
