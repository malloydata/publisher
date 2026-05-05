import { Add } from "@mui/icons-material";
import {
   Box,
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   Snackbar,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import { Package } from "../../client";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

interface AddPackageDialogProps {
   resourceUri: string;
}

export default function AddPackageDialog({
   resourceUri,
}: AddPackageDialogProps) {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();
   const queryClient = useQueryClient();
   const [notificationMessage, setNotificationMessage] = useState("");

   const { projectName } = parseResourceUri(resourceUri);

   const addPackage = useMutationWithApiError({
      async mutationFn(variables: Package) {
         return apiClients.packages.createPackage(projectName, {
            name: variables.name,
            description: variables.description,
            location: variables.location,
         });
      },
      onSuccess() {
         setOpen(false);
         setNotificationMessage("Package created successfully");
         queryClient.invalidateQueries({ queryKey: ["packages", projectName] });
      },
      onError(error) {
         setNotificationMessage(
            error instanceof Error
               ? error.message
               : "An unknown error occurred",
         );
      },
   });

   const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const name = formData.get("name")?.toString();
      const description = formData.get("description")?.toString();
      const location = formData.get("location")?.toString();
      addPackage.mutate({ name, description, location });
   };

   return (
      <>
         <Button
            onClick={() => setOpen(true)}
            variant="contained"
            color="primary"
            startIcon={<Add />}
         >
            Add Package
         </Button>

         <Dialog
            open={open}
            onClose={() => setOpen(false)}
            maxWidth="sm"
            fullWidth
            PaperProps={{ sx: { borderRadius: 2 } }}
         >
            <DialogTitle
               sx={{
                  fontSize: "1.25rem",
                  fontWeight: 600,
                  letterSpacing: "-0.025em",
                  pt: 3,
                  pb: 1,
                  px: 3,
               }}
            >
               Create new package
            </DialogTitle>
            <DialogContent sx={{ px: 3, pb: 0 }}>
               <Box sx={{ mb: 3 }}>
                  <Typography
                     variant="body2"
                     color="text.secondary"
                     sx={{ mb: 1.5 }}
                  >
                     Create a new Malloy package to start exploring your data.
                  </Typography>
                  <Typography
                     variant="body2"
                     color="text.secondary"
                     sx={{ mb: 1.5 }}
                  >
                     The location can be a GitHub/S3/GCP URL containing a
                     package (zipped or unzipped), or an absolute path to a
                     directory the publisher server has access to.
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                     Make sure to conform to the{" "}
                     <Box
                        component="a"
                        href="https://github.com/malloydata/publisher/blob/main/README.md#architecture-overview"
                        target="_blank"
                        rel="noopener noreferrer"
                        sx={{
                           color: "text.primary",
                           textDecoration: "underline",
                        }}
                     >
                        Malloy Package Format
                     </Box>
                     .
                  </Typography>
               </Box>
               <form onSubmit={handleSubmit} id="package-form">
                  <Stack spacing={2.5}>
                     <TextField
                        autoFocus
                        required
                        id="name"
                        name="name"
                        label="Package name"
                        type="text"
                        fullWidth
                        variant="outlined"
                        size="small"
                        InputLabelProps={{ shrink: true }}
                     />
                     <TextField
                        id="description"
                        name="description"
                        label="Description"
                        multiline
                        rows={3}
                        fullWidth
                        variant="outlined"
                        size="small"
                        InputLabelProps={{ shrink: true }}
                     />
                     <TextField
                        id="location"
                        name="location"
                        label="Location"
                        type="text"
                        placeholder="e.g. s3://my-bucket/my-package.zip"
                        fullWidth
                        variant="outlined"
                        size="small"
                        InputLabelProps={{ shrink: true }}
                     />
                  </Stack>
               </form>
            </DialogContent>
            <DialogActions sx={{ px: 3, pt: 2, pb: 3, gap: 1 }}>
               <Button
                  variant="outlined"
                  disabled={addPackage.isPending}
                  onClick={() => setOpen(false)}
               >
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="package-form"
                  variant="contained"
                  loading={addPackage.isPending}
               >
                  Create package
               </Button>
            </DialogActions>
         </Dialog>
         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </>
   );
}
