// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** A document the host created; `where` names which storage took the write, as on the `*.saved` events. */
export type DashboardCreatedEvent = {
   type: "dashboard.created";
   where: "package" | "host";
};

export type NotebookCreatedEvent = {
   type: "notebook.created";
   where: "package" | "host";
};

export type DocumentCreatedEvent = DashboardCreatedEvent | NotebookCreatedEvent;
