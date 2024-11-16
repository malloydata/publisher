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

axios.defaults.baseURL = "http://localhost:4000";
const packagesApi = new PackagesApi(new Configuration());
const queryClient = new QueryClient();

interface PackageProps {
   server?: string;
   packageName: string;
   versionId?: string;
}

export default function Package({
   server,
   packageName,
   versionId,
}: PackageProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: ["package", server, packageName, versionId],
         queryFn: () =>
            packagesApi.getPackage(packageName, versionId, {
               baseURL: server,
               withCredentials: true,
            }),
         retry: false,
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
                  maxHeight: "300px",
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
                     <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
                        {`${packageName} > ${versionId} - ${error.message}`}
                     </Typography>
                  )}
               </List>
            </Box>
         </StyledCardContent>
      </StyledCard>
   );
}