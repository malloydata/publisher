// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import CalculateOutlinedIcon from "@mui/icons-material/CalculateOutlined";
import FunctionsIcon from "@mui/icons-material/Functions";
import LabelOutlinedIcon from "@mui/icons-material/LabelOutlined";
import LinkIcon from "@mui/icons-material/Link";
import ViewListOutlinedIcon from "@mui/icons-material/ViewListOutlined";
import { Stack } from "@mui/material";
import * as React from "react";
import { ItemRow } from "../ItemRow";
import { PackageSection } from "../PackageSection";
import { docOf } from "../ReportBuilder/viewAnnotations";
import { PALETTE } from "../styles";

/**
 * What each kind of field is called and drawn as. Exhaustive over Malloy's
 * field kinds, so a kind added upstream fails to compile here rather than
 * rendering as nothing.
 */
const FIELD_KIND: Record<
   Malloy.FieldInfoType,
   { noun: string; tint: string; icon: React.ReactNode }
> = {
   dimension: {
      noun: "dimension",
      tint: PALETTE.cyan,
      icon: <LabelOutlinedIcon fontSize="small" />,
   },
   measure: {
      noun: "measure",
      tint: PALETTE.lime,
      icon: <FunctionsIcon fontSize="small" />,
   },
   calculate: {
      noun: "calculation",
      tint: PALETTE.amber,
      icon: <CalculateOutlinedIcon fontSize="small" />,
   },
   view: {
      noun: "view",
      tint: PALETTE.violet,
      icon: <ViewListOutlinedIcon fontSize="small" />,
   },
   join: {
      noun: "join",
      tint: PALETTE.slate,
      icon: <LinkIcon fontSize="small" />,
   },
};

/** `string_type` -> `string`; a join names its relationship instead. */
function fieldDetail(field: Malloy.FieldInfo): string {
   const { noun } = FIELD_KIND[field.kind];
   switch (field.kind) {
      case "dimension":
      case "measure":
      case "calculate":
         return `${noun} · ${field.type.kind.replace(/_type$/, "")}`;
      case "join":
         return `${noun} · ${field.relationship}`;
      case "view":
         return noun;
   }
}

/**
 * Each source a model exposes, with its fields, read-only.
 *
 * The model page's way to see what a model offers now that the Explorer is
 * gone. It is deliberately the least that answers "what is in here": the model
 * viewer that replaces it is a port of a fuller one, and should replace this
 * outright rather than grow out of it.
 */
export function ModelSources({
   sourceInfos,
}: {
   sourceInfos: Malloy.SourceInfo[];
}) {
   return (
      <Stack component="div">
         {sourceInfos.map((source) => (
            <PackageSection
               key={source.name}
               title={source.name}
               count={source.schema.fields.length}
               description={docOf(source.annotations?.map((a) => a.value))}
            >
               {source.schema.fields.map((field) => (
                  <ItemRow
                     key={field.name}
                     icon={FIELD_KIND[field.kind].icon}
                     tint={FIELD_KIND[field.kind].tint}
                     label={field.name}
                     mono
                     description={docOf(field.annotations?.map((a) => a.value))}
                     rightLabel={fieldDetail(field)}
                  />
               ))}
            </PackageSection>
         ))}
      </Stack>
   );
}
