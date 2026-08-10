import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import { Box, IconButton, Stack, Tooltip, Typography } from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import { MONO_FONT_FAMILY } from "../styles";
import { parseResourceUri } from "../../utils/formatting";
import {
   isPublisherResizeMessage,
   packageFileUrl,
} from "../../utils/dataAppEmbed";

interface DataAppViewerProps {
   resourceUri: string;
}

function parseDataAppResource(resourceUri: string) {
   try {
      const { environmentName, packageName, modelPath } =
         parseResourceUri(resourceUri);
      if (!environmentName || !packageName || !modelPath) return null;
      return { environmentName, packageName, dataAppPath: modelPath };
   } catch {
      return null;
   }
}

/**
 * Renders an in-package HTML data app (from the Publisher static-file route)
 * inside an iframe, wrapped in light SPA chrome — title, breadcrumb-ish
 * label, and an "open standalone" escape hatch.
 *
 * The iframe `src` points at the Publisher worker's standalone URL on the
 * data origin (derived from `useServer().server`, which may differ from
 * the SPA origin in multi-host deployments). The app's `publisher.js`
 * runtime postMessages `{ type: "publisher:resize", height }` as content
 * height changes; we listen and resize the iframe to match so embedded
 * dashboards don't get a nested scrollbar.
 *
 * Full-screen apps (e.g. slide decks) can opt out of content-height sizing
 * with `<meta name="publisher:fit" content="viewport">`, surfaced as
 * `Page.fit === "viewport"`. The iframe then fills the available viewport
 * height (matching the standalone view) instead of collapsing to the app's
 * near-zero reported content height.
 */
export default function DataAppViewer({ resourceUri }: DataAppViewerProps) {
   const { server, apiClients } = useServer();
   const parsed = parseDataAppResource(resourceUri);
   const environmentName = parsed?.environmentName ?? "";
   const packageName = parsed?.packageName ?? "";
   const dataAppPath = parsed?.dataAppPath ?? "";

   const standaloneUrl = useMemo(
      () =>
         packageFileUrl({
            server,
            environmentName,
            packageName,
            path: dataAppPath,
         }),
      [server, environmentName, packageName, dataAppPath],
   );

   // Use the /pages endpoint to grab the <title> for the header. Any error
   // here (older Publisher without /pages, transient network blip, 5xx) is
   // non-fatal — `title` falls back to `dataAppPath` and the iframe still
   // renders. The app itself is loaded via the iframe `src`, not from this
   // query, so blocking the viewer on a metadata lookup would be wrong.
   const pagesQuery = useQueryWithApiError({
      queryKey: ["pages", environmentName, packageName],
      queryFn: () => apiClients.pages.listPages(environmentName, packageName),
      enabled: !!parsed,
   });
   const dataAppMeta = pagesQuery.data?.data?.find(
      (a) => a.path === dataAppPath,
   );
   const title = dataAppMeta?.title ?? dataAppPath;
   // Full-screen apps opt in via <meta name="publisher:fit" content="viewport">
   // (surfaced as Page.fit by the /pages listing). In fill mode the
   // iframe fills the available viewport height instead of being sized to the
   // app's reported content height: a viewport-filling deck has ~no content
   // height to report, so the default auto-size would clip it. An ordinary
   // content-driven app keeps auto-sizing.
   const fillViewport = dataAppMeta?.fit === "viewport";

   const iframeRef = useRef<HTMLIFrameElement | null>(null);
   // Start small so the iframe doesn't pre-commit space before the first
   // resize message; the runtime postMessages the real content height
   // within a frame of load.
   const [iframeHeight, setIframeHeight] = useState<number>(120);

   useEffect(() => {
      // Fill mode pins the iframe to 100%, so content-height resize messages are
      // irrelevant; skip subscribing (and the per-message re-renders) until the
      // app is in content-height mode.
      if (fillViewport) return;
      function onMessage(e: MessageEvent) {
         if (!isPublisherResizeMessage(e.data)) return;
         if (e.source !== iframeRef.current?.contentWindow) return;
         if (e.data.height > 0) {
            setIframeHeight(Math.ceil(e.data.height));
         }
      }
      window.addEventListener("message", onMessage);
      return () => window.removeEventListener("message", onMessage);
   }, [fillViewport]);

   if (!parsed) {
      return (
         <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
               Can&apos;t open data app
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
               This data app link is missing an environment, package, or path:
               <Box
                  component="span"
                  sx={{ fontFamily: MONO_FONT_FAMILY, ml: 1 }}
               >
                  {resourceUri}
               </Box>
            </Typography>
         </Box>
      );
   }

   return (
      <Box
         sx={{
            p: 3,
            // Fill mode fills the available height of DataAppViewer's ancestor,
            // so it must render inside a height-constrained container. The
            // Publisher app provides one (MainPage's 100dvh flex chain); an SDK
            // consumer embedding DataAppViewer should give it a bounded-height
            // parent.
            ...(fillViewport
               ? { height: "100%", display: "flex", flexDirection: "column" }
               : { maxWidth: 1200, mx: "auto" }),
         }}
      >
         <Stack
            direction="row"
            alignItems="baseline"
            spacing={1}
            sx={{ mb: 1, flexShrink: 0 }}
         >
            <Typography
               variant="h6"
               sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
            >
               {title}
            </Typography>
            <Typography
               variant="caption"
               color="text.secondary"
               sx={{ fontFamily: MONO_FONT_FAMILY }}
            >
               {dataAppPath}
            </Typography>
            <Box sx={{ flex: 1 }} />
            <Tooltip title="Open standalone in new tab">
               <IconButton
                  size="small"
                  href={standaloneUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="Open standalone in new tab"
                  sx={{ color: "text.secondary" }}
               >
                  <OpenInNewIcon fontSize="small" />
               </IconButton>
            </Tooltip>
         </Stack>
         <Box
            sx={{
               border: "1px solid",
               borderColor: "divider",
               borderRadius: 1.5,
               overflow: "hidden",
               backgroundColor: "background.paper",
               // Fill mode: grow to consume the remaining viewport height (the
               // SPA gives DataAppViewer a 100dvh flex ancestor) so the iframe
               // below can be 100% tall. minHeight:0 lets this flex child
               // actually shrink/grow instead of overflowing its parent.
               ...(fillViewport ? { flex: 1, minHeight: 0 } : {}),
            }}
         >
            <iframe
               ref={iframeRef}
               src={standaloneUrl}
               title={title}
               // allow-same-origin so the iframe can use cookies for the
               // Publisher API. allow-scripts so the dashboard's JS runs.
               // allow-forms so dropdown/filter forms work. Intentionally
               // NOT allowing top-navigation or popups.
               sandbox="allow-scripts allow-same-origin allow-forms"
               style={{
                  display: "block",
                  width: "100%",
                  // Fill mode pins the iframe to the available height so the
                  // deck's own 100vh resolves against a real viewport. Otherwise
                  // track the content height reported via publisher:resize.
                  height: fillViewport ? "100%" : iframeHeight,
                  border: 0,
               }}
            />
         </Box>
      </Box>
   );
}
