// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createContext, useContext, type ReactNode } from "react";
import type { GivenRequest } from "./modelGivens";
import type { ShownRun } from "./RunResult";
import type { ExplorerField } from "./schemaModel";
import type { SampleValuesFetcher } from "./useDimensionSamples";

/** An open view's or named query's run, for a host's actions beside it. */
export interface RunActionContext {
   kind: "view" | "query";
   modelPath: string;
   /** The source a view belongs to; "" for a named query. */
   sourceName: string;
   name: string;
   /**
    * The Malloy the row runs: the view or query by name, with no row limit of
    * its own, so its rows are what the server's cap allows.
    */
   query: string;
   /** The given values the run sends (`QueryRequest.givens`), unset ones left out. */
   givens?: GivenRequest;
   /**
    * The viewer's cache key for this run, so a host's refresh can invalidate
    * it and a host's own run of the same query can share the entry.
    */
   queryKey: readonly unknown[];
   /** The result the card shows, once it has one. */
   shown?: ShownRun;
}

/** A field row, for a host's additions to it. */
export interface FieldContext {
   modelPath: string;
   sourceName: string;
   field: ExplorerField;
   /** A joined source's field, shown inside a join on another source's page. */
   joined: boolean;
}

/** The source on screen, or a source in the picker, for a host's additions to it. */
export interface SourceContext {
   modelPath: string;
   sourceName: string;
}

/** What a host adds to a source's line in the picker. */
export interface PickerSourceExtras {
   /** A description where the source has no `#(doc)`, marked Generated. */
   summary?: string;
   /** Hold the description's line: the host is still reading its summaries. */
   summaryPending?: boolean;
   /** Chips after the source's name. */
   status?: ReactNode;
}

/**
 * What a host adds to the viewer without the viewer knowing what it is: the
 * Publisher's own reads draw every row, and a host with more to say about a
 * field (what it has built from it, an index of its values) or more to do with
 * a result (download it) says and does it through these.
 *
 * **Experimental.** These slots are new, and no host has been built on them
 * yet; they may change in a minor release until one has.
 */
export interface ModelViewerExtensions {
   /** Actions beside an open view's or named query's Result | Code switch. */
   renderRunActions?: (run: RunActionContext) => ReactNode;
   /** A dimension's sample values, instead of the viewer's own query. */
   fetchSamples?: SampleValuesFetcher;
   /**
    * The build of the values `fetchSamples` serves (an index's version, say):
    * a new one is a new cache entry, so a row reopened after a rebuild never
    * shows the values from before it.
    */
   samplesGeneration?: string;
   /** Chips a host adds to a field's line, after its own. */
   renderFieldStatus?: (field: FieldContext) => ReactNode;
   /** The row's one action, beside its chips (a Rebuild, say). */
   renderFieldAction?: (field: FieldContext) => ReactNode;
   /**
    * What an open field row shows. `builtIn` is the viewer's own card (the
    * field's code, a dimension's values, the givens it reads), undefined where
    * the row has none. Return it to keep it, beside a card of your own or not;
    * return something else to show that in its place; return null for nothing,
    * and the row then doesn't open.
    */
   renderFieldDetail?: (
      field: FieldContext,
      builtIn: ReactNode | undefined,
   ) => ReactNode;
   /**
    * Whether a view's or field's row is listed, for a host's narrowed view of
    * a source (the fields it has built from, say). Each section's count is of
    * the rows it lists. Absent, every row is.
    */
   showField?: (field: FieldContext) => boolean;
   /** A card under the source's header, for what only the host knows of it (where its data comes from, its build). */
   renderSourceDetail?: (source: SourceContext) => ReactNode;
   /** A host's summary and chips on a source's line in the picker. */
   describePickerSource?: (
      source: SourceContext,
   ) => PickerSourceExtras | undefined;
}

export const ModelViewerExtensionsContext =
   createContext<ModelViewerExtensions>({});

export function useModelViewerExtensions(): ModelViewerExtensions {
   return useContext(ModelViewerExtensionsContext);
}
