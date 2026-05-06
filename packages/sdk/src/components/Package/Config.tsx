import ErrorIcon from "@mui/icons-material/ErrorOutlined";
import { Box, List, ListItem, ListItemText } from "@mui/material";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

type Props = {
   resourceUri: string;
};

export default function Config({ resourceUri }: Props) {
   const { apiClients } = useServer();
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);

   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["package", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.packages.getPackage(
            environmentName,
            packageName,
            versionId,
            false,
         ),
   });

   return (
      <PackageCard>
         <PackageCardContent>
            <PackageSectionTitle>Package Config</PackageSectionTitle>
            <Box
               sx={{
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               <List dense={true} disablePadding={true}>
                  <ListItem dense={true} disablePadding={true}>
                     <ListItemText
                        primary="Name"
                        primaryTypographyProps={{ fontWeight: "500" }}
                        secondary={packageName}
                     />
                  </ListItem>
                  {!isSuccess && !isError && (
                     <ListItem>
                        <Loading text="Fetching Package Metadata..." />
                     </ListItem>
                  )}
                  {isSuccess &&
                     ((data.data && (
                        <ListItem dense={true} disablePadding={true}>
                           <ListItemText
                              primary="Description"
                              primaryTypographyProps={{
                                 fontWeight: "500",
                              }}
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
                        context={`${environmentName} > ${packageName} > ${versionId}`}
                     />
                  )}
               </List>
            </Box>
         </PackageCardContent>
      </PackageCard>
   );
}
