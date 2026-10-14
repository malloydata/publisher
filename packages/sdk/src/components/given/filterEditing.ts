// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createContext, useContext } from "react";

/**
 * What the report builder adds to a report's row of filter buttons, so the
 * row a reader uses is also the one place a filter is configured: no second
 * row of chips naming the same filters again.
 *
 * Provided by the builder around the host's control row; absent everywhere
 * else, where the row is the reader's and offers no editing at all.
 */
export interface FilterEditing {
   /** The builder's word on one filter, by given name; undefined when it does not know the name. */
   stateOf: (name: string) => FilterEditState | undefined;
   /**
    * Filters the file declares that the row has no control for (no tile reads
    * them yet), shown after the row's own so they can still be configured or
    * removed.
    */
   unrendered: { name: string; label: string }[];
   /** Open a filter's settings: which tiles it applies to, its label and kind. */
   onEdit: (name: string) => void;
   /** Take a filter off the report. */
   onRemove: (name: string) => void;
   /** Declare a new filter, or bind one the model offers. */
   onAdd: () => void;
   /** Why adding is off; set, the add button is disabled and says why. */
   addDisabledReason?: string;
}

export interface FilterEditState {
   /** Why a tile cannot take this filter's binding, when one cannot. */
   problem?: string;
   /** Where the filter is declared: this file, or the model it imports. */
   origin: "dashboard" | "model";
   /**
    * Set when a tile reads the filter through the model rather than a binding
    * in this file: why it can be set but not removed or configured here.
    */
   fixed?: string;
}

export const FilterEditingContext = createContext<FilterEditing | undefined>(
   undefined,
);

/** The builder's filter editing, when the row is drawn inside the builder. */
export const useFilterEditing = () => useContext(FilterEditingContext);
