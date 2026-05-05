import { ReactElement } from "react";

/**
 * Public API surface used by `MalloyPublisherApp({ headerProps })` consumers.
 *
 * As of v0.0.195, the shell is a permanent left sidebar + per-page content
 * header (see MainPage / Sidebar). The slot positions have changed since
 * v0.0.194; the type signature is unchanged but rendering location is not:
 *
 * - `logoHeader` overrides the default Malloy wordmark in the sidebar
 *   header (slot is 56 px tall; sidebar is 260 px wide expanded, 64 px
 *   wide collapsed, so wide horizontal wordmarks may be cropped or hidden
 *   when collapsed). Prefer compact mark + short label, or a single icon
 *   that still reads at 64 px.
 *
 * - `endCap` renders into the content header's `#header-actions-portal`
 *   (right-aligned slot in a 56 px content header). Use it for primary
 *   page-level actions that should follow the user across routes (e.g. a
 *   "Build Model" or "Sign in" button). The slot is shared across all
 *   routes, so don't put per-route actions here — those belong on the
 *   page itself.
 */
export interface HeaderProps {
   logoHeader?: ReactElement;
   endCap?: ReactElement;
}

/**
 * Stub retained so that `import Header from ".../Header"` keeps working
 * for any consumer who imported it. The real shell is rendered by
 * MainPage + Sidebar; this component renders nothing.
 */
export default function Header(_props: HeaderProps) {
   return null;
}
