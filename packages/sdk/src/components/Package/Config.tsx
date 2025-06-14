import ErrorIcon from "@mui/icons-material/ErrorOutlined";
import {
   Box,
   Divider,
   List,
   ListItem,
   ListItemText,
   Typography,
} from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";
import axios from "axios";
import { Configuration, PackagesApi } from "../../client";
import { StyledCard, StyledCardContent } from "../styles";
import { usePackage } from "./PackageProvider";
import { ApiErrorDisplay } from "../ApiErrorDisplay";

const packagesApi = new PackagesApi(new Configuration());
const queryClient = new QueryClient();

export default function Config() {
   const { server, projectName, packageName, versionId, accessToken } =
      usePackage();

   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: ["package", server, projectName, packageName, versionId],
         queryFn: () =>
            packagesApi.getPackage(projectName, packageName, versionId, false, {
               baseURL: server,
               withCredentials: !accessToken,
               headers: {
                  Authorization: accessToken && `Bearer ${accessToken}`,
               },
            }),
         retry: false,
         throwOnError: false,
      },
      queryClient,
   );

   return (
      <StyledCard variant="outlined" sx={{ padding: "10px", width: "100%" }}>
         <StyledCardContent>
            <Typography variant="overline" fontWeight="bold">
               Package Config
            </Typography>
            <Divider />
            <Box
               sx={{
                  mt: "10px",
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               <List dense={true} disablePadding={true}>
                  <ListItem dense={true} disablePadding={true}>
                     <ListItemText primary="Name" secondary={packageName} />
                  </ListItem>
                  {!isSuccess && !isError && (
                     <Typography variant="body2" sx={{ p: "20px", m: "auto" }}>
                        Fethching Package Metadata...
                     </Typography>
                  )}
                  {isSuccess &&
                     ((data.data && (
                        <ListItem dense={true} disablePadding={true}>
                           <ListItemText
                              primary="Description"
                              secondary={data.data.description}
                           />
                        </ListItem>
                     )) || (
                        <ListItem
                           disablePadding={true}
                           dense={true}
                           sx={{ mt: "20px" }}
                        >
                           <ErrorIcon
                              sx={{
                                 color: "grey.600",
                                 mr: "10px",
                              }}
                           />
                           <ListItemText primary={"No package manifest"} />
                        </ListItem>
                     ))}
                  {isError && (
                     <ApiErrorDisplay
                        error={error}
                        context={`${projectName} > ${packageName} > ${versionId}`}
                     />
                  )}
               </List>
            </Box>
         </StyledCardContent>
      </StyledCard>
   );
}
