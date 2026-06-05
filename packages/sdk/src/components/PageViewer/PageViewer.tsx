import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import { Box, IconButton, Stack, Tooltip, Typography } from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import { MONO_FONT_FAMILY } from "../styles";
import { parseResourceUri } from "../../utils/formatting";
import { isPublisherResizeMessage, serverBaseUrl } from "../../utils/pageEmbed";

interface PageViewerProps {
   resourceUri: string;
}

function parsePageResource(resourceUri: string) {
   try {
      const { environmentName, packageName, modelPath } =
         parseResourceUri(resourceUri);
      if (!environmentName || !packageName || !modelPath) return null;
      return { environmentName, packageName, pagePath: modelPath };
   } catch {
      return null;
   }
}

/**
 * Renders an in-package HTML page (from the Publisher static-file route)
 * inside an iframe, wrapped in light SPA chrome — title, breadcrumb-ish
 * label, and an "open standalone" escape hatch.
 *
 * The iframe `src` points at the Publisher worker's standalone URL on the
 * data origin (derived from `useServer().server`, which may differ from
 * the SPA origin in multi-host deployments). The page's `publisher.js`
 * runtime postMessages `{ type: "publisher:resize", height }` as content
 * height changes; we listen and resize the iframe to match so embedded
 * dashboards don't get a nested scrollbar.
 */
export default function PageViewer({ resourceUri }: PageViewerProps) {
   const { server, apiClients } = useServer();
   const parsed = parsePageResource(resourceUri);
   const environmentName = parsed?.environmentName ?? "";
   const packageName = parsed?.packageName ?? "";
   const pagePath = parsed?.pagePath ?? "";

   const standaloneUrl = useMemo(() => {
      return `${serverBaseUrl(server)}/environments/${encodeURIComponent(
         environmentName,
      )}/packages/${encodeURIComponent(packageName)}/${pagePath}`;
   }, [server, environmentName, packageName, pagePath]);

   // Use the /pages endpoint to grab the <title> for the header. Any error
   // here (older Publisher without /pages, transient network blip, 5xx) is
   // non-fatal — `title` falls back to `pagePath` and the iframe still
   // renders. The page itself is loaded via the iframe `src`, not from this
   // query, so blocking the viewer on a metadata lookup would be wrong.
   const pagesQuery = useQueryWithApiError({
      queryKey: ["pages", environmentName, packageName],
      queryFn: () => apiClients.pages.listPages(environmentName, packageName),
      enabled: !!parsed,
   });
   const title =
      pagesQuery.data?.data?.find((p) => p.path === pagePath)?.title ??
      pagePath;

   const iframeRef = useRef<HTMLIFrameElement | null>(null);
   // Start small so the iframe doesn't pre-commit space before the first
   // resize message; the runtime postMessages the real content height
   // within a frame of load.
   const [iframeHeight, setIframeHeight] = useState<number>(120);

   useEffect(() => {
      function onMessage(e: MessageEvent) {
         if (!isPublisherResizeMessage(e.data)) return;
         if (e.source !== iframeRef.current?.contentWindow) return;
         if (e.data.height > 0) {
            setIframeHeight(Math.ceil(e.data.height));
         }
      }
      window.addEventListener("message", onMessage);
      return () => window.removeEventListener("message", onMessage);
   }, []);

   if (!parsed) {
      return (
         <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
               Can&apos;t open page
            </Typography>
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
               This page link is missing an environment, package, or path:
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
      <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
         <Stack
            direction="row"
            alignItems="baseline"
            spacing={1}
            sx={{ mb: 1 }}
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
               {pagePath}
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
                  height: iframeHeight,
                  border: 0,
               }}
            />
         </Box>
      </Box>
   );
}
