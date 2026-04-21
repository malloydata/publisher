import CloseIcon from "@mui/icons-material/Close";
import FileUploadOutlinedIcon from "@mui/icons-material/FileUploadOutlined";
import {
   Button,
   Chip,
   Divider,
   FormControl,
   Menu,
   MenuItem,
   Stack,
   Typography,
} from "@mui/material";
import React from "react";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

interface ModelPickerProps {
   initialSelectedModels: string[];
   onModelChange: (models: string[]) => void;
   resourceUri: string;
}

/**
 * Picks Models that are available in the package.
 * Only "malloy" model files are shown.
 */
export function ModelPicker({
   initialSelectedModels,
   onModelChange,
   resourceUri,
}: ModelPickerProps) {
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   const { data, isLoading, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["models", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.models.listModels(environmentName, packageName, versionId),
   });
   const [selectedModels, setSelectedModels] = React.useState<string[]>(
      initialSelectedModels || [],
   );
   const [anchorEl, setAnchorEl] = React.useState<null | HTMLElement>(null);

   React.useEffect(() => {
      setSelectedModels(initialSelectedModels || []);
   }, [initialSelectedModels]);

   const handleRemove = (model: string) => {
      const newModels = selectedModels.filter((m) => m !== model);
      setSelectedModels(newModels);
      onModelChange(newModels);
   };

   const handleAdd = (model: string) => {
      const newModels = [...selectedModels, model];
      setSelectedModels(newModels);
      onModelChange(newModels);
      setAnchorEl(null);
   };

   let availableModels: string[] = [];
   if (isSuccess && data?.data) {
      availableModels = data.data
         .filter((model) => !selectedModels.includes(model.path))
         .map((model) => model.path);
   }

   if (isError) {
      return (
         <ApiErrorDisplay
            error={error}
            context={`${environmentName} > ${packageName} > Model Picker`}
         />
      );
   }

   return (
      <>
         <FormControl fullWidth>
            {isLoading && <Typography>Loading...</Typography>}
            <Stack direction="row" spacing={1}>
               {selectedModels.map((model) => (
                  <Chip
                     key={model}
                     label={model}
                     onDelete={() => handleRemove(model)}
                     deleteIcon={<CloseIcon />}
                     sx={{ marginBottom: "5px" }}
                  />
               ))}
               <Button
                  size="small"
                  startIcon={<FileUploadOutlinedIcon />}
                  onClick={(e) => setAnchorEl(e.currentTarget)}
                  disabled={
                     isLoading || isError || availableModels.length === 0
                  }
               >
                  Add Model
               </Button>
            </Stack>
         </FormControl>
         <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={() => setAnchorEl(null)}
         >
            <Typography variant="subtitle2" sx={{ ml: 1 }}>
               Models
            </Typography>
            <Divider />
            {availableModels.length === 0 ? (
               <MenuItem disabled>No models available</MenuItem>
            ) : (
               availableModels.map((model) => (
                  <MenuItem key={model} onClick={() => handleAdd(model)}>
                     <Typography variant="body2">{model}</Typography>
                  </MenuItem>
               ))
            )}
         </Menu>
      </>
   );
}
