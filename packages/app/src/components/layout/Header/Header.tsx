import { ReactElement } from "react";

/**
 * Public API surface used by `MalloyPublisherApp({ headerProps })` consumers.
 * The shell is now a left sidebar + content header (see MainPage / Sidebar);
 * `logoHeader` overrides the default brand mark in the sidebar header,
 * `endCap` renders into the content header's actions slot.
 */
export interface HeaderProps {
   logoHeader?: ReactElement;
   endCap?: ReactElement;
}

export default function Header(_props: HeaderProps) {
   return null;
}
