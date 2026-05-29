import { Add, Launch } from "@mui/icons-material";
import {
   Box,
   Button,
   Dialog,
   DialogContent,
   DialogTitle,
   FormControl,
   InputLabel,
   ListItemIcon,
   ListItemText,
   Menu,
   MenuItem,
   Select,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import React from "react";
import { WorkbookList } from "./Workbook";
import { useWorkbookStorage } from "./Workbook/WorkbookStorageProvider";
import type { WorkbookLocator, Workspace } from "./Workbook/WorkbookStorage";

export interface AnalyzePackageButtonProps {
   onWorkbookSelect: (
      workbook: WorkbookLocator,
      event: React.MouseEvent,
   ) => void;
}

export function AnalyzePackageButton({
   onWorkbookSelect,
}: AnalyzePackageButtonProps) {
   const { workbookStorage } = useWorkbookStorage();
   const [workbookName, setWorkbookName] = React.useState("");
   const [selectedWorkspace, setSelectedWorkspace] = React.useState("");
   const [availableWorkspaces, setAvailableWorkspaces] = React.useState<
      Workspace[]
   >([]);
   const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null);
   const [newDialogOpen, setNewDialogOpen] = React.useState(false);
   const [openDialogOpen, setOpenDialogOpen] = React.useState(false);

   const open = Boolean(anchorEl);
   const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
      setAnchorEl(event.currentTarget);
   };
   const handleMenuClose = () => {
      setAnchorEl(null);
   };
   const handleOpenDialogClose = () => {
      setOpenDialogOpen(false);
   };
   const handleNewDialogClose = () => {
      setNewDialogOpen(false);
      setWorkbookName("");
      setSelectedWorkspace("");
   };

   // Fetch available workspaces when the new dialog opens
   React.useEffect(() => {
      if (workbookStorage) {
         console.log("fetching workspaces");
         workbookStorage
            .listWorkspaces(true) // Only show writeable workspaces
            .then((workspaces) => {
               setAvailableWorkspaces(workspaces);
               // If only one workspace, select it by default
               if (workspaces.length === 1) {
                  setSelectedWorkspace(workspaces[0].name);
               }
            })
            .catch((error) => {
               console.error("Error fetching workspaces:", error);
               setAvailableWorkspaces([]);
            });
      }
   }, [workbookStorage]);

   const handleWorkbookClick = (
      workbook: WorkbookLocator,
      event: React.MouseEvent,
   ) => {
      setOpenDialogOpen(false);
      onWorkbookSelect(workbook, event);
   };

   const createWorkbookClick = (event?: React.MouseEvent) => {
      setNewDialogOpen(false);
      onWorkbookSelect(
         {
            path: workbookName,
            workspace: selectedWorkspace,
         },
         event,
      );
      setWorkbookName("");
      setSelectedWorkspace("");
   };

   const noWorkspaces = availableWorkspaces.length === 0;
   return (
      <>
         <Button
            aria-controls={open ? "basic-menu" : undefined}
            aria-haspopup="true"
            aria-expanded={open ? "true" : undefined}
            onClick={handleClick}
            variant="contained"
            sx={{
               height: "40px",
               px: 2,
               backgroundColor: "#fbbb04",
               "&:hover": {
                  backgroundColor: "#eab308",
               },
            }}
         >
            Analyze Package
         </Button>
         <Menu
            id="basic-menu"
            anchorEl={anchorEl}
            open={open}
            onClose={handleMenuClose}
            MenuListProps={{
               "aria-labelledby": "basic-button",
               sx: { py: 0.5 },
            }}
         >
            <MenuItem
               onClick={() => {
                  setNewDialogOpen(true);
                  handleMenuClose();
               }}
               sx={{ py: 1, px: 2 }}
               disabled={noWorkspaces}
            >
               <ListItemIcon>
                  <Add fontSize="small" />
               </ListItemIcon>
               <ListItemText>
                  <Typography variant="body2" fontWeight={500}>
                     New Workbook
                  </Typography>
                  {noWorkspaces ? (
                     <Typography variant="caption" color="text.secondary">
                        No workspaces available
                     </Typography>
                  ) : (
                     <Typography variant="caption" color="text.secondary">
                        Create a new analysis workbook
                     </Typography>
                  )}
               </ListItemText>
            </MenuItem>
            <MenuItem
               onClick={() => {
                  setOpenDialogOpen(true);
                  handleMenuClose();
               }}
               sx={{ py: 1, px: 2 }}
            >
               <ListItemIcon>
                  <Launch fontSize="small" />
               </ListItemIcon>
               <ListItemText>
                  <Typography variant="body2" fontWeight={500}>
                     Open Workbook
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                     Open an existing workbook
                  </Typography>
               </ListItemText>
            </MenuItem>
         </Menu>

         {/* Create New Workbook Dialog */}
         <Dialog
            open={newDialogOpen}
            onClose={handleNewDialogClose}
            maxWidth="sm"
            fullWidth
         >
            <DialogTitle sx={{ pb: 1, pt: 2, px: 2 }}>
               <Typography
                  fontWeight={600}
                  sx={{ fontSize: "1.5rem", mb: 0.5 }}
               >
                  Create New Workbook
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Start a new analysis workbook to explore your data
               </Typography>
            </DialogTitle>
            <DialogContent sx={{ px: 2, pb: 2 }}>
               <Stack spacing={2} sx={{ mt: 1 }}>
                  {availableWorkspaces.length > 1 ? (
                     <FormControl fullWidth size="small">
                        <InputLabel>Workspace</InputLabel>
                        <Select
                           value={selectedWorkspace}
                           label="Workspace"
                           onChange={(e) =>
                              setSelectedWorkspace(e.target.value)
                           }
                        >
                           {availableWorkspaces.map((workspace) => (
                              <MenuItem
                                 key={workspace.name}
                                 value={workspace.name}
                              >
                                 <Box>
                                    <Typography
                                       variant="body2"
                                       fontWeight={500}
                                    >
                                       {workspace.name}
                                    </Typography>
                                    <Typography
                                       variant="caption"
                                       color="text.secondary"
                                    >
                                       {workspace.description}
                                    </Typography>
                                 </Box>
                              </MenuItem>
                           ))}
                        </Select>
                     </FormControl>
                  ) : availableWorkspaces.length === 1 ? (
                     <Box
                        sx={(theme) => ({
                           p: 2,
                           border: 1,
                           borderColor: "divider",
                           borderRadius: 1,
                           backgroundColor:
                              theme.palette.mode === "dark"
                                 ? "rgba(255, 255, 255, 0.04)"
                                 : "grey.50",
                        })}
                     >
                        <Typography
                           variant="body2"
                           fontWeight={500}
                           gutterBottom
                        >
                           Workspace: {availableWorkspaces[0].name}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                           {availableWorkspaces[0].description}
                        </Typography>
                     </Box>
                  ) : null}
                  <FormControl fullWidth>
                     <TextField
                        label="Workbook Name"
                        value={workbookName}
                        onChange={(e) => setWorkbookName(e.target.value)}
                        placeholder="Enter workbook name..."
                        fullWidth
                        autoFocus
                        size="small"
                     />
                  </FormControl>
                  <Stack direction="row" spacing={1} justifyContent="flex-end">
                     <Button
                        onClick={handleNewDialogClose}
                        variant="outlined"
                        size="small"
                     >
                        Cancel
                     </Button>
                     <Button
                        onClick={(event) => createWorkbookClick(event)}
                        variant="contained"
                        disabled={
                           !workbookName.trim() ||
                           (availableWorkspaces.length > 1 &&
                              !selectedWorkspace)
                        }
                        size="small"
                     >
                        Create Workbook
                     </Button>
                  </Stack>
               </Stack>
            </DialogContent>
         </Dialog>

         {/* Open Workbook Dialog */}
         <Dialog
            open={openDialogOpen}
            onClose={handleOpenDialogClose}
            maxWidth="md"
            fullWidth
         >
            <DialogTitle sx={{ pb: 1, pt: 2, px: 2 }}>
               <Typography
                  fontWeight={600}
                  sx={{ mb: 0.5, fontSize: "1.5rem" }}
               >
                  Open Workbook
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Select an existing workbook to continue your analysis
               </Typography>
            </DialogTitle>
            <DialogContent sx={{ px: 2, pb: 2 }}>
               <WorkbookList onWorkbookClick={handleWorkbookClick} />
            </DialogContent>
         </Dialog>
      </>
   );
}
