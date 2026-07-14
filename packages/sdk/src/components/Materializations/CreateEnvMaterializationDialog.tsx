import AddIcon from "@mui/icons-material/Add";
import {
   Box,
   Button,
   Checkbox,
   Dialog,
   DialogActions,
   DialogContent,
   DialogContentText,
   DialogTitle,
   FormControlLabel,
   FormGroup,
   Switch,
   Typography,
} from "@mui/material";
import { useState } from "react";

type CreateEnvMaterializationDialogProps = {
   /** Packages in the environment the user can materialize. */
   packages: { name: string }[];
   onSubmit: (opts: {
      packageNames: string[];
      forceRefresh: boolean;
   }) => Promise<unknown>;
   isSubmitting: boolean;
   disabled?: boolean;
};

export default function CreateEnvMaterializationDialog({
   packages,
   onSubmit,
   isSubmitting,
   disabled,
}: CreateEnvMaterializationDialogProps) {
   const [open, setOpen] = useState(false);
   const [selected, setSelected] = useState<Record<string, boolean>>({});
   const [forceRefresh, setForceRefresh] = useState(false);

   const handleOpen = () => {
      setSelected({});
      setForceRefresh(false);
      setOpen(true);
   };
   const handleClose = () => setOpen(false);

   const selectedNames = packages
      .map((p) => p.name)
      .filter((name) => selected[name]);

   const toggle = (name: string) =>
      setSelected((prev) => ({ ...prev, [name]: !prev[name] }));

   const allSelected =
      packages.length > 0 && selectedNames.length === packages.length;

   const handleRun = async () => {
      try {
         await onSubmit({ packageNames: selectedNames, forceRefresh });
         setOpen(false);
      } catch {
         // The caller surfaces the error through its Snackbar; keep the dialog
         // open so the user can retry or adjust the selection.
      }
   };

   return (
      <>
         <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleOpen}
            disabled={disabled}
            aria-label="New materialization"
         >
            New materialization
         </Button>

         <Dialog
            open={open}
            onClose={handleClose}
            maxWidth="xs"
            fullWidth
            aria-labelledby="create-env-materialization-title"
         >
            <DialogTitle id="create-env-materialization-title">
               New materialization
            </DialogTitle>
            <DialogContent>
               <DialogContentText sx={{ mb: 1 }}>
                  Pick the packages to materialize. Each runs an independent
                  build of every persist source in that package.
               </DialogContentText>

               {packages.length === 0 ? (
                  <Typography variant="body2" color="text.secondary">
                     No packages in this environment.
                  </Typography>
               ) : (
                  <>
                     <FormControlLabel
                        control={
                           <Checkbox
                              checked={allSelected}
                              indeterminate={
                                 selectedNames.length > 0 && !allSelected
                              }
                              onChange={(event) => {
                                 const next: Record<string, boolean> = {};
                                 if (event.target.checked) {
                                    packages.forEach(
                                       (p) => (next[p.name] = true),
                                    );
                                 }
                                 setSelected(next);
                              }}
                           />
                        }
                        label={<Typography variant="body2">Select all</Typography>}
                     />
                     <Box
                        sx={{
                           maxHeight: 240,
                           overflowY: "auto",
                           pl: 1,
                           borderLeft: "2px solid",
                           borderColor: "divider",
                        }}
                     >
                        <FormGroup>
                           {packages.map((p) => (
                              <FormControlLabel
                                 key={p.name}
                                 control={
                                    <Checkbox
                                       checked={!!selected[p.name]}
                                       onChange={() => toggle(p.name)}
                                    />
                                 }
                                 label={p.name}
                              />
                           ))}
                        </FormGroup>
                     </Box>
                     <FormControlLabel
                        sx={{ mt: 1 }}
                        control={
                           <Switch
                              checked={forceRefresh}
                              onChange={(event) =>
                                 setForceRefresh(event.target.checked)
                              }
                           />
                        }
                        label="Force refresh (rebuild even if unchanged)"
                     />
                  </>
               )}
            </DialogContent>
            <DialogActions>
               <Button onClick={handleClose}>Cancel</Button>
               <Button
                  variant="contained"
                  loading={isSubmitting}
                  disabled={selectedNames.length === 0}
                  onClick={handleRun}
               >
                  Materialize
                  {selectedNames.length > 0 ? ` (${selectedNames.length})` : ""}
               </Button>
            </DialogActions>
         </Dialog>
      </>
   );
}
