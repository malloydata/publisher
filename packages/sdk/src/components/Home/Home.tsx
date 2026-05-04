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
import { Project } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { getProjectDescription } from "../../utils/parsing";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import AddProjectDialog from "./AddProjectDialog";
import DeleteProjectDialog from "./DeleteProjectDialog";
import EditProjectDialog from "./EditProjectDialog";

interface HomeProps {
   onClickProject?: (to: string, event?: React.MouseEvent) => void;
}

const FEATURES: Array<{ title: string; body: string; href: string }> = [
   {
      title: "Ad-hoc analysis",
      body: "Browse semantic sources, build queries, and run nested logic in Explorer — no code.",
      href: "https://github.com/malloydata/publisher/blob/main/README.md#ad-hoc-data-analysis",
   },
   {
      title: "Notebook dashboards",
      body: "Code-first dashboards using Malloy notebooks. Versioned alongside your models.",
      href: "https://github.com/malloydata/publisher/blob/main/README.md#notebook-based-dashboards",
   },
   {
      title: "AI data agents",
      body: "Expose models via MCP so agents can discover sources and ask well-formed questions.",
      href: "https://github.com/malloydata/publisher/blob/main/README.md#mcp-based-ai-data-agents",
   },
];

export default function Home({ onClickProject }: HomeProps) {
   const { apiClients, mutable } = useServer();

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["projects"],
      queryFn: () => apiClients.projects.listProjects(),
   });

   if (isError) {
      return <ApiErrorDisplay error={error} context="Projects List" />;
   }

   if (!isSuccess) {
      return <Loading text="Loading projects..." />;
   }

   const projects = data.data ?? [];

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
            <Typography
               variant="body1"
               color="text.secondary"
               sx={{ mb: 3 }}
            >
               The open-source semantic model server for the Malloy data
               language.
            </Typography>
            <Typography
               variant="body2"
               color="text.secondary"
               sx={{ maxWidth: 720, lineHeight: 1.6 }}
            >
               Define semantic models once — and use them everywhere. Publisher
               serves Malloy models through clean APIs, enabling consistent,
               interpretable, and AI-ready data access for tools, applications,
               and agents.
            </Typography>
         </Box>

         <Grid container spacing={4} sx={{ mb: 5 }}>
            {FEATURES.map((feature) => (
               <Grid size={{ xs: 12, md: 4 }} key={feature.title}>
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

         {projects.length > 0 ? (
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
                        Projects
                     </Typography>
                     <Typography variant="body2" color="text.secondary">
                        Published projects available on this server
                     </Typography>
                  </Box>
                  {mutable && <AddProjectDialog />}
               </Stack>
               <Grid container spacing={2}>
                  {projects.map((project) => (
                     <Grid
                        size={{ xs: 12, sm: 6, md: 4 }}
                        key={project.name}
                     >
                        <ProjectCard
                           project={project}
                           onClickProject={onClickProject}
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
                  Create your first Malloy project to start exploring semantic
                  models and building data experiences.
               </Typography>
               {mutable ? (
                  <AddProjectDialog />
               ) : (
                  <Button
                     variant="contained"
                     color="primary"
                     href="https://github.com/malloydata/publisher/blob/main/README.md#server-configuration"
                     target="_blank"
                     rel="noopener noreferrer"
                  >
                     Learn how to create models
                  </Button>
               )}
            </Box>
         )}

         <Divider sx={{ my: 4 }} />

         <Typography variant="body2" color="text.secondary">
            Publisher is built on fully open infrastructure and designed for the
            AI era. Join the{" "}
            <Box
               component="a"
               href="https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw"
               target="_blank"
               rel="noopener noreferrer"
               sx={{
                  color: "text.primary",
                  textDecoration: "underline",
               }}
            >
               Malloy Slack community
            </Box>{" "}
            to ask questions, share ideas, and contribute.
         </Typography>
      </Container>
   );
}

function ProjectCard({
   project,
   onClickProject,
}: {
   project: Project;
   onClickProject?: (to: string, event?: React.MouseEvent) => void;
}) {
   const { mutable } = useServer();
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const menuOpen = Boolean(menuAnchorEl);

   const description = getProjectDescription(project.readme);

   const handleClick = (event: React.MouseEvent) => {
      if (project.name && onClickProject) {
         onClickProject(`/${project.name}/`, event);
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
                  sx={{
                     width: 36,
                     height: 36,
                     borderRadius: 1.5,
                     bgcolor: "grey.100",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                     flexShrink: 0,
                     color: "text.primary",
                  }}
               >
                  <FolderOutlinedIcon sx={{ fontSize: 20 }} />
               </Box>
               <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                     variant="subtitle1"
                     noWrap
                     sx={{ fontWeight: 600, mb: 0.5 }}
                  >
                     {project.name}
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
                        aria-label="Project options"
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
                        <EditProjectDialog
                           project={project}
                           onCloseDialog={handleMenuClose}
                        />
                        <DeleteProjectDialog
                           project={project}
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
