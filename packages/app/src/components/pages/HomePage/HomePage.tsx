// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Home, useRouterClickHandler } from "@malloy-publisher/sdk";

export default function HomePage() {
   const navigate = useRouterClickHandler();
   return <Home onClickEnvironment={navigate} />;
}
