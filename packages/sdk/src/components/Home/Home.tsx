import { MoreVert } from "@mui/icons-material";
import FolderOutlinedIcon from "@mui/icons-material/FolderOutlined";
import {
   Box,
   Button,
   Card,
   CardContent,
   Container,
   Divider,
   Grid,
   IconButton,
   Menu,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import { useState } from "react";
import { Environment } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { getEnvironmentDescription } from "../../utils/parsing";
import { DOC_LINKS } from "../../constants/docLinks";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import AddEnvironmentDialog from "./AddEnvironmentDialog";
import DeleteEnvironmentDialog from "./DeleteEnvironmentDialog";
import EditEnvironmentDialog from "./EditEnvironmentDialog";

interface HomeProps {
   onClickEnvironment?: (to: string, event?: React.MouseEvent) => void;
}

/**
 * What this server can do, in the order someone meets it: exploring first, then
 * the three artifacts a package can hold, then the endpoint agents come in
 * through and the rules that govern all of it. Anything that does not earn a
 * card is named in the closing paragraph rather than crowding this list.
 */
const FEATURES: Array<{ title: string; body: string; href: string }> = [
   {
      title: "Ad-hoc analysis",
      body: "Browse the semantic sources on this server and build nested queries in Explorer. Every click writes valid Malloy, so metrics stay correct even across joins.",
      href: DOC_LINKS.explorer,
   },
   {
      title: "Notebooks",
      body: "A data story: markdown and live query cells in one file, versioned beside the model it reads, behind a panel of filter controls the model defines.",
      href: DOC_LINKS.surfaces,
   },
   {
      title: "Dashboards",
      body: "A tagged Malloy file is the page. Tags lay out the grid, the filter row is rendered from the query's parameters, and a tagged dimension clicks through to a detail page. No front-end code.",
      href: DOC_LINKS.dashboards,
   },
   {
      title: "Data apps",
      body: "Hand-author an HTML page in a package's public directory and Publisher serves it, backed by that package's models. No build step, and it embeds in a host page.",
      href: DOC_LINKS.dataApps,
   },
   {
      title: "AI data agents",
      body: "One MCP endpoint. Agents discover the sources, compile-check the Malloy they write, and ask well-formed questions instead of guessing at raw tables.",
      href: DOC_LINKS.mcpAgents,
   },
   {
      title: "Governed access",
      body: "Runtime parameters, row-level filters, and per-source access gates are declared in the model, so every surface and every caller inherits the same rules.",
      href: DOC_LINKS.givens,
   },
];

function InlineLink({
   href,
   external = true,
   children,
}: {
   href: string;
   /** The API spec is served by this same server, so it stays in the tab. */
   external?: boolean;
   children: React.ReactNode;
}) {
   return (
      <Box
         component="a"
         href={href}
         target={external ? "_blank" : undefined}
         rel={external ? "noopener noreferrer" : undefined}
         sx={{ color: "text.primary", textDecoration: "underline" }}
      >
         {children}
      </Box>
   );
}

export default function Home({ onClickEnvironment }: HomeProps) {
   const { apiClients, mutable } = useServer();

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["environments"],
      queryFn: () => apiClients.environments.listEnvironments(),
   });

   if (isError) {
      return <ApiErrorDisplay error={error} context="Environments List" />;
   }

   if (!isSuccess) {
      return <Loading text="Loading environments..." />;
   }

   const environments = data.data ?? [];

   return (
      <Container maxWidth="md" sx={{ py: 6 }}>
         <Box sx={{ mb: 5 }}>
            <Typography
               variant="h3"
               component="h1"
               sx={{ fontWeight: 500, letterSpacing: "-0.025em", mb: 1 }}
            >
               Publisher
            </Typography>
            <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
               The open-source semantic model server for the Malloy data
               language.
            </Typography>
            <Typography
               variant="body2"
               color="text.secondary"
               sx={{ maxWidth: 720, lineHeight: 1.6 }}
            >
               Define semantic models once — and use them everywhere. Publisher
               serves Malloy models over a REST API and a single MCP endpoint,
               so applications, BI tools, and AI agents compose queries against
               the model instead of writing SQL, and the numbers come back right
               by construction.
            </Typography>
         </Box>

         <Grid container spacing={4} sx={{ mb: 5 }}>
            {FEATURES.map((feature) => (
               <Grid size={{ xs: 12, sm: 6 }} key={feature.title}>
                  <Stack spacing={1}>
                     <Typography
                        variant="body2"
                        component="a"
                        href={feature.href}
                        target="_blank"
                        rel="noopener noreferrer"
                        sx={{
                           fontWeight: 500,
                           color: "text.primary",
                           textDecoration: "none",
                           "&:hover": { textDecoration: "underline" },
                        }}
                     >
                        {feature.title}
                     </Typography>
                     <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ lineHeight: 1.6 }}
                     >
                        {feature.body}
                     </Typography>
                  </Stack>
               </Grid>
            ))}
         </Grid>

         <Divider sx={{ my: 4 }} />

         {environments.length > 0 ? (
            <Box sx={{ mb: 4 }}>
               <Stack
                  direction="row"
                  justifyContent="space-between"
                  alignItems="flex-start"
                  sx={{ mb: 3 }}
               >
                  <Box>
                     <Typography
                        variant="h5"
                        sx={{
                           fontWeight: 500,
                           letterSpacing: "-0.025em",
                           mb: 0.5,
                        }}
                     >
                        Environments
                     </Typography>
                     <Typography variant="body2" color="text.secondary">
                        Published environments available on this server
                     </Typography>
                  </Box>
                  {mutable && <AddEnvironmentDialog />}
               </Stack>
               <Grid container spacing={2}>
                  {environments.map((environment) => (
                     <Grid
                        size={{ xs: 12, sm: 6, md: 4 }}
                        key={environment.name}
                     >
                        <EnvironmentCard
                           environment={environment}
                           onClickEnvironment={onClickEnvironment}
                        />
                     </Grid>
                  ))}
               </Grid>
            </Box>
         ) : (
            <Box sx={{ mb: 4 }}>
               <Typography
                  variant="h5"
                  sx={{ fontWeight: 500, letterSpacing: "-0.025em", mb: 1 }}
               >
                  Get started
               </Typography>
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mb: 3, maxWidth: 600 }}
               >
                  Create your first Malloy environment to start exploring
                  semantic models and building data experiences.
               </Typography>
               {mutable ? (
                  <AddEnvironmentDialog />
               ) : (
                  <Button
                     variant="contained"
                     color="primary"
                     href={DOC_LINKS.publishing}
                     target="_blank"
                     rel="noopener noreferrer"
                  >
                     Learn how to create models
                  </Button>
               )}
            </Box>
         )}

         <Divider sx={{ my: 4 }} />

         <Typography
            variant="body2"
            color="text.secondary"
            sx={{ maxWidth: 720, lineHeight: 1.6 }}
         >
            Also here:{" "}
            <InlineLink href={DOC_LINKS.connections}>connections</InlineLink> to
            BigQuery, Snowflake, Postgres, MySQL, Trino, and DuckDB;{" "}
            <InlineLink href={DOC_LINKS.materialization}>
               materialized tables
            </InlineLink>{" "}
            built on demand or on a schedule; and a{" "}
            <InlineLink href="/api-doc.html" external={false}>
               REST API
            </InlineLink>{" "}
            that does everything this console does.
         </Typography>

         <Typography
            variant="body2"
            color="text.secondary"
            sx={{ maxWidth: 720, lineHeight: 1.6, mt: 2 }}
         >
            Publisher is built on fully open infrastructure and designed for the
            AI era. Join the{" "}
            <InlineLink href="https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw">
               Malloy Slack community
            </InlineLink>{" "}
            to ask questions, share ideas, and contribute.
         </Typography>
      </Container>
   );
}

function EnvironmentCard({
   environment,
   onClickEnvironment,
}: {
   environment: Environment;
   onClickEnvironment?: (to: string, event?: React.MouseEvent) => void;
}) {
   const { mutable } = useServer();
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const menuOpen = Boolean(menuAnchorEl);

   const description = getEnvironmentDescription(environment.readme);

   const handleClick = (event: React.MouseEvent) => {
      if (environment.name && onClickEnvironment) {
         onClickEnvironment(`/${environment.name}/`, event);
      }
   };

   const handleMenuClick = (event: React.MouseEvent<HTMLElement>) => {
      event.stopPropagation();
      setMenuAnchorEl(event.currentTarget);
   };

   const handleMenuClose = () => {
      setMenuAnchorEl(null);
   };

   return (
      <Card
         variant="outlined"
         onClick={handleClick}
         sx={{
            height: "100%",
            cursor: "pointer",
            borderRadius: 3,
            borderColor: "divider",
            boxShadow: "none",
            transition: "all 0.2s ease-in-out",
            "&:hover": { boxShadow: 2, borderColor: "primary.main" },
         }}
      >
         <CardContent sx={{ p: 2.5, "&:last-child": { pb: 2.5 } }}>
            <Box
               sx={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: 1.5,
               }}
            >
               <Box
                  sx={(theme) => ({
                     width: 36,
                     height: 36,
                     borderRadius: 1.5,
                     bgcolor:
                        theme.palette.mode === "dark"
                           ? "rgba(255, 255, 255, 0.08)"
                           : "grey.100",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                     flexShrink: 0,
                     color: "text.primary",
                  })}
               >
                  <FolderOutlinedIcon sx={{ fontSize: 20 }} />
               </Box>
               <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                     variant="subtitle1"
                     component="h6"
                     noWrap
                     sx={{ fontWeight: 600, mb: 0.5 }}
                  >
                     {environment.name}
                  </Typography>
                  <Tooltip title={description} followCursor enterDelay={1000}>
                     <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{
                           overflow: "hidden",
                           textOverflow: "ellipsis",
                           display: "-webkit-box",
                           WebkitLineClamp: 2,
                           WebkitBoxOrient: "vertical",
                           lineHeight: 1.5,
                        }}
                     >
                        {description}
                     </Typography>
                  </Tooltip>
               </Box>
               {mutable && (
                  <>
                     <IconButton
                        size="small"
                        onClick={handleMenuClick}
                        aria-label={`Environment actions for ${environment.name}`}
                        sx={{ flexShrink: 0, mt: -0.5, mr: -0.5 }}
                     >
                        <MoreVert fontSize="small" />
                     </IconButton>
                     <Menu
                        anchorEl={menuAnchorEl}
                        open={menuOpen}
                        onClose={handleMenuClose}
                        onClick={(e) => e.stopPropagation()}
                        anchorOrigin={{
                           vertical: "bottom",
                           horizontal: "right",
                        }}
                        transformOrigin={{
                           vertical: "top",
                           horizontal: "right",
                        }}
                     >
                        <EditEnvironmentDialog
                           environment={environment}
                           onCloseDialog={handleMenuClose}
                        />
                        <DeleteEnvironmentDialog
                           environment={environment}
                           onCloseDialog={handleMenuClose}
                        />
                     </Menu>
                  </>
               )}
            </Box>
         </CardContent>
      </Card>
   );
}
