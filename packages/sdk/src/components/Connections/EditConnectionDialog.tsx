import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import React, { useState } from "react";

import { Edit } from "@mui/icons-material";
import MenuItem from "@mui/material/MenuItem";
import {
   AttachedDatabase,
   AttachedDatabaseTypeEnum,
   Connection,
   ConnectionTypeEnum,
   DucklakeConnection,
} from "../../client/api";
import {
   attachedDatabaseConnectionFieldName,
   attributesFieldName,
   connectionFieldsByType,
   gcsAttachedDatabaseFields,
   getAttachedDatabaseFields,
   s3AttachedDatabaseFields,
} from "./common";

type EditConnectionDialogProps = {
   connection: Connection;
   onSubmit: (connection: Connection) => Promise<unknown>;
   isSubmitting: boolean;
};

function initAttachedDatabases(connection: Connection) {
   const dbs = connection.duckdbConnection?.attachedDatabases;
   if (!dbs || dbs.length === 0) return [];
   return dbs.map((db) => ({
      name: db.name || "",
      dbType: (db.type || "postgres") as AttachedDatabaseTypeEnum,
   }));
}

function initDucklakeStorageType(connection: Connection): string {
   const dl = connection.ducklakeConnection;
   if (!dl) return "s3";
   if (dl.storage?.gcsConnection) return "gcs";
   return "s3";
}

export default function EditConnectionDialog({
   connection,
   onSubmit,
   isSubmitting,
}: EditConnectionDialogProps) {
   const [open, setOpen] = useState(false);
   const [type, setType] = useState<Connection["type"]>(connection.type);
   const [attachedDatabases, setAttachedDatabases] = useState<
      Array<{
         name: string;
         dbType: AttachedDatabaseTypeEnum;
         [key: string]: string | AttachedDatabaseTypeEnum | undefined;
      }>
   >(() => initAttachedDatabases(connection));

   // DuckLake top-level connection state
   const [ducklakeCatalogType, setDucklakeCatalogType] = useState("postgres");
   const [ducklakeStorageType, setDucklakeStorageType] = useState(() =>
      initDucklakeStorageType(connection),
   );

   const handleClickOpen = () => {
      setAttachedDatabases(initAttachedDatabases(connection));
      setType(connection.type);
      setDucklakeCatalogType("postgres");
      setDucklakeStorageType(initDucklakeStorageType(connection));
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
   };

   const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();

      const formData = new FormData(event.currentTarget);
      const name = formData.get("name")?.toString();
      const type = formData.get("type")?.toString() as ConnectionTypeEnum;
      const fields = connectionFieldsByType[type];
      if (!name) {
         throw new Error("Name is required");
      }
      if (!type) {
         throw new Error("Type is required");
      }

      let connectionPayload: Connection;

      if (type === "ducklake") {
         // Build catalog connection
         const catalogConfig: Record<string, unknown> = {};
         if (ducklakeCatalogType === "postgres") {
            const pgFields = connectionFieldsByType["postgres"];
            const pgConfig: Record<string, string> = {};
            pgFields.forEach((field) => {
               const value = formData
                  .get(`ducklake_pg_${field.name}`)
                  ?.toString();
               if (value) {
                  pgConfig[field.name] = value;
               }
            });
            if (Object.keys(pgConfig).length === 0) {
               throw new Error("DuckLake requires catalog connection");
            }
            catalogConfig.postgresConnection = pgConfig;
         }

         // Build storage connection
         const bucketUrl = formData.get("ducklake_bucketUrl")?.toString();
         if (!bucketUrl) {
            throw new Error("DuckLake requires a bucket URL");
         }
         const storageConfig: Record<string, unknown> = { bucketUrl };

         if (ducklakeStorageType === "s3") {
            const s3Config: Record<string, string> = {};
            s3AttachedDatabaseFields.forEach((field) => {
               const value = formData
                  .get(`ducklake_s3_${field.name}`)
                  ?.toString();
               if (value) {
                  s3Config[field.name] = value;
               }
            });
            // For updates, use existing values if not provided
            const existingS3 =
               connection.ducklakeConnection?.storage?.s3Connection;
            if (existingS3) {
               if (!s3Config.accessKeyId && existingS3.accessKeyId) {
                  s3Config.accessKeyId = existingS3.accessKeyId;
               }
               if (!s3Config.secretAccessKey && existingS3.secretAccessKey) {
                  s3Config.secretAccessKey = existingS3.secretAccessKey;
               }
            }
            // Validate required fields
            if (!s3Config.accessKeyId) {
               throw new Error("S3 Access Key ID is required");
            }
            if (!s3Config.secretAccessKey) {
               throw new Error("S3 Secret Access Key is required");
            }
            if (Object.keys(s3Config).length > 0) {
               storageConfig.s3Connection = s3Config;
            }
         } else if (ducklakeStorageType === "gcs") {
            const gcsConfig: Record<string, string> = {};
            gcsAttachedDatabaseFields.forEach((field) => {
               const value = formData
                  .get(`ducklake_gcs_${field.name}`)
                  ?.toString();
               if (value) {
                  gcsConfig[field.name] = value;
               }
            });
            // For updates, use existing values if not provided
            const existingGcs =
               connection.ducklakeConnection?.storage?.gcsConnection;
            if (existingGcs) {
               if (!gcsConfig.keyId && existingGcs.keyId) {
                  gcsConfig.keyId = existingGcs.keyId;
               }
               if (!gcsConfig.secret && existingGcs.secret) {
                  gcsConfig.secret = existingGcs.secret;
               }
            }
            // Validate required fields
            if (!gcsConfig.keyId) {
               throw new Error("GCS Key ID is required");
            }
            if (!gcsConfig.secret) {
               throw new Error("GCS Secret is required");
            }
            if (Object.keys(gcsConfig).length > 0) {
               storageConfig.gcsConnection = gcsConfig;
            }
         }

         connectionPayload = {
            name,
            type,
            ducklakeConnection: {
               catalog:
                  catalogConfig as unknown as DucklakeConnection["catalog"],
               storage:
                  storageConfig as unknown as DucklakeConnection["storage"],
            },
         } satisfies Connection;
      } else if (type === "duckdb") {
         if (attachedDatabases.length === 0) {
            throw new Error(
               "DuckDB connection must have at least one attached database",
            );
         }

         const attachedDbs: AttachedDatabase[] = attachedDatabases.map(
            (db, index) => {
               const dbType = db.dbType;
               const dbName = formData
                  .get(`attachedDb_${index}_name`)
                  ?.toString();
               if (!dbName) {
                  throw new Error(
                     `Attached database at index ${index} must have a name`,
                  );
               }

               const attachedDb: AttachedDatabase = {
                  name: dbName,
                  type: dbType,
               };

               const dbFields = getAttachedDatabaseFields(dbType);
               if (dbFields && dbFields.length > 0) {
                  const connectionConfig: Record<string, string> = {};
                  dbFields.forEach((field) => {
                     const value = formData
                        .get(`attachedDb_${index}_${field.name}`)
                        ?.toString();
                     if (value) {
                        connectionConfig[field.name] = value;
                     }
                  });

                  const connectionFieldName =
                     attachedDatabaseConnectionFieldName[dbType];
                  if (connectionFieldName) {
                     (attachedDb as AttachedDatabase)[connectionFieldName] =
                        connectionConfig;
                  }
               }

               return attachedDb;
            },
         );

         connectionPayload = {
            name,
            type,
            duckdbConnection: {
               attachedDatabases: attachedDbs,
            },
         } satisfies Connection;
      } else {
         // Regular connection types
         const existingConfig = connection[attributesFieldName[type]] || {};
         const connectionConfig: Record<string, string> = {};

         fields.forEach((field) => {
            const formValue = formData.get(field.name)?.toString();
            const existingValue = existingConfig[field.name];

            // For password/secret fields, use existing value if form value is empty
            const isPasswordField =
               field.type === "password" ||
               field.name === "password" ||
               field.name === "secretAccessKey" ||
               field.name === "secret" ||
               field.name === "accessToken" ||
               field.name === "privateKey";

            if (formValue) {
               connectionConfig[field.name] = formValue;
            } else if (isPasswordField && existingValue) {
               // Keep existing password/secret if not provided
               connectionConfig[field.name] = existingValue;
            } else if (formValue !== undefined) {
               connectionConfig[field.name] = formValue;
            } else if (existingValue) {
               connectionConfig[field.name] = existingValue;
            }
         });

         // Validate required fields based on connection type
         if (type === "postgres") {
            const hasConnectionString =
               !!connectionConfig.connectionString?.trim();
            if (!hasConnectionString) {
               // All detailed fields are required if no connection string
               const requiredFields = [
                  "host",
                  "port",
                  "databaseName",
                  "userName",
                  "password",
               ];
               for (const fieldName of requiredFields) {
                  if (!connectionConfig[fieldName]) {
                     throw new Error(
                        `${fields.find((f) => f.name === fieldName)?.label || fieldName} is required`,
                     );
                  }
               }
            }
         } else if (type === "bigquery") {
            if (!connectionConfig.serviceAccountKeyJson) {
               throw new Error("Service Account Key JSON is required");
            }
         } else if (type === "snowflake") {
            if (!connectionConfig.account) {
               throw new Error("Account is required");
            }
            if (!connectionConfig.username) {
               throw new Error("Username is required");
            }
            if (!connectionConfig.warehouse) {
               throw new Error("Warehouse is required");
            }
            if (!connectionConfig.password && !connectionConfig.privateKey) {
               throw new Error("Either password or private key is required");
            }
         } else if (type === "trino") {
            if (!connectionConfig.server) {
               throw new Error("Server is required");
            }
            if (!connectionConfig.user) {
               throw new Error("User is required");
            }
            // Password is required for HTTPS unless peakaKey is used
            const server = connectionConfig.server.trim();
            if (
               server.startsWith("https://") &&
               !connectionConfig.password &&
               !connectionConfig.peakaKey
            ) {
               throw new Error(
                  "Password is required for HTTPS connections (or use Peaka Key)",
               );
            }
         } else if (type === "mysql") {
            const requiredFields = [
               "host",
               "port",
               "database",
               "user",
               "password",
            ];
            for (const fieldName of requiredFields) {
               if (!connectionConfig[fieldName]) {
                  throw new Error(
                     `${fields.find((f) => f.name === fieldName)?.label || fieldName} is required`,
                  );
               }
            }
         } else if (type === "motherduck") {
            if (!connectionConfig.accessToken) {
               throw new Error("Access Token is required");
            }
         }

         connectionPayload = {
            name,
            type,
            [attributesFieldName[type]]: connectionConfig,
         } satisfies Connection;
      }

      await onSubmit(connectionPayload);
      handleClose();
   };

   const addAttachedDatabase = () => {
      setAttachedDatabases([
         ...attachedDatabases,
         { name: "", dbType: "postgres" as AttachedDatabaseTypeEnum },
      ]);
   };

   const removeAttachedDatabase = (index: number) => {
      setAttachedDatabases(attachedDatabases.filter((_, i) => i !== index));
   };

   const updateAttachedDatabaseType = (
      index: number,
      dbType: AttachedDatabaseTypeEnum,
   ) => {
      const updated = [...attachedDatabases];
      updated[index] = { ...updated[index], dbType };
      setAttachedDatabases(updated);
   };

   // Helper to get existing attached database field default values
   const getAttachedDbDefault = (index: number, fieldName: string): string => {
      const existingDbs = connection.duckdbConnection?.attachedDatabases;
      if (!existingDbs || !existingDbs[index]) return "";
      const db = existingDbs[index] as AttachedDatabase;
      const connFieldName = attachedDatabaseConnectionFieldName[db.type || ""];
      if (connFieldName && db[connFieldName]) {
         return db[connFieldName][fieldName] ?? "";
      }
      return "";
   };

   // Helper to get existing ducklake connection field default values
   const getDucklakeDefault = (fieldPath: string): string => {
      const dl = connection.ducklakeConnection as DucklakeConnection;
      if (!dl) return "";
      if (fieldPath.startsWith("pg_")) {
         const fieldName = fieldPath.replace("pg_", "");
         return dl.catalog?.postgresConnection?.[fieldName] ?? "";
      }
      if (fieldPath === "bucketUrl") {
         return dl.storage?.bucketUrl ?? "";
      }
      if (fieldPath.startsWith("s3_")) {
         const fieldName = fieldPath.replace("s3_", "");
         return dl.storage?.s3Connection?.[fieldName] ?? "";
      }
      if (fieldPath.startsWith("gcs_")) {
         const fieldName = fieldPath.replace("gcs_", "");
         return dl.storage?.gcsConnection?.[fieldName] ?? "";
      }
      return "";
   };

   return (
      <React.Fragment>
         <IconButton
            aria-label={`Edit connection ${connection?.name ?? ""}`.trim()}
            onClick={(event) => {
               event.preventDefault();
               event.stopPropagation();
               handleClickOpen();
            }}
         >
            <Edit />
         </IconButton>
         <Dialog open={open} onClose={handleClose}>
            <DialogTitle
               onClick={(event) => {
                  event.stopPropagation();
               }}
            >
               Edit Connection
            </DialogTitle>
            <DialogContent
               onClick={(event) => {
                  event.stopPropagation();
               }}
            >
               <DialogContentText>
                  Edit a connection to query your data database using Malloy.
               </DialogContentText>
               <form onSubmit={handleSubmit} id="connection-form">
                  <TextField
                     autoFocus
                     required
                     margin="dense"
                     id="name"
                     name="name"
                     label="Connection Name"
                     type="text"
                     fullWidth
                     variant="standard"
                     defaultValue={connection.name}
                  />
                  <TextField
                     margin="dense"
                     id="type"
                     name="type"
                     label="Connection Type"
                     fullWidth
                     variant="standard"
                     value={type}
                     select
                     onChange={(event) =>
                        setType(event.target.value as ConnectionTypeEnum)
                     }
                  >
                     {Object.values(ConnectionTypeEnum).map((type) => (
                        <MenuItem key={type} value={type}>
                           {type}
                        </MenuItem>
                     ))}
                  </TextField>
                  {type === "ducklake" ? (
                     <Box sx={{ mt: 2 }}>
                        <Box
                           sx={{
                              mb: 3,
                              p: 2,
                              border: "1px solid",
                              borderColor: "divider",
                              borderRadius: 1,
                           }}
                        >
                           <Typography
                              variant="subtitle2"
                              sx={{ mb: 2 }}
                              fontWeight={500}
                           >
                              Catalog
                           </Typography>
                           <TextField
                              margin="dense"
                              id="ducklake_catalogType"
                              label="Catalog Type"
                              fullWidth
                              variant="standard"
                              value={ducklakeCatalogType}
                              select
                              onChange={(event) =>
                                 setDucklakeCatalogType(event.target.value)
                              }
                           >
                              <MenuItem value="postgres">PostgreSQL</MenuItem>
                           </TextField>
                           {ducklakeCatalogType === "postgres" && (
                              <>
                                 {connectionFieldsByType["postgres"].map(
                                    (field) => (
                                       <TextField
                                          key={`pg_${field.name}`}
                                          margin="dense"
                                          id={`ducklake_pg_${field.name}`}
                                          name={`ducklake_pg_${field.name}`}
                                          label={field.label}
                                          type={field.type}
                                          fullWidth
                                          variant="standard"
                                          defaultValue={getDucklakeDefault(
                                             `pg_${field.name}`,
                                          )}
                                       />
                                    ),
                                 )}
                              </>
                           )}
                        </Box>
                        <Box
                           sx={{
                              mb: 3,
                              p: 2,
                              border: "1px solid",
                              borderColor: "divider",
                              borderRadius: 1,
                           }}
                        >
                           <Typography
                              variant="subtitle2"
                              sx={{ mb: 2 }}
                              fontWeight={500}
                           >
                              Storage
                           </Typography>
                           <TextField
                              margin="dense"
                              id="ducklake_storageType"
                              label="Storage Type"
                              fullWidth
                              variant="standard"
                              value={ducklakeStorageType}
                              select
                              onChange={(event) =>
                                 setDucklakeStorageType(event.target.value)
                              }
                           >
                              <MenuItem value="s3">S3</MenuItem>
                              <MenuItem value="gcs">GCS</MenuItem>
                           </TextField>
                           <TextField
                              margin="dense"
                              required
                              id="ducklake_bucketUrl"
                              name="ducklake_bucketUrl"
                              label={
                                 ducklakeStorageType === "s3"
                                    ? "Bucket URL (e.g. s3://my-bucket/path)"
                                    : "Bucket URL (e.g. gs://my-bucket/path)"
                              }
                              type="text"
                              fullWidth
                              variant="standard"
                              defaultValue={getDucklakeDefault("bucketUrl")}
                           />
                           {ducklakeStorageType === "s3" && (
                              <>
                                 <Typography
                                    variant="caption"
                                    sx={{ mt: 2, mb: 1, display: "block" }}
                                    color="text.secondary"
                                 >
                                    S3 Credentials
                                 </Typography>
                                 {s3AttachedDatabaseFields.map((field) => (
                                    <TextField
                                       key={`s3_${field.name}`}
                                       margin="dense"
                                       id={`ducklake_s3_${field.name}`}
                                       name={`ducklake_s3_${field.name}`}
                                       label={field.label}
                                       type={field.type}
                                       fullWidth
                                       variant="standard"
                                       required={
                                          field.required &&
                                          field.name !== "secretAccessKey"
                                       }
                                       defaultValue={getDucklakeDefault(
                                          `s3_${field.name}`,
                                       )}
                                       placeholder={
                                          field.name === "region"
                                             ? "us-east-1"
                                             : field.name === "secretAccessKey"
                                               ? "Leave empty to keep existing"
                                               : undefined
                                       }
                                    />
                                 ))}
                              </>
                           )}
                           {ducklakeStorageType === "gcs" && (
                              <>
                                 <Typography
                                    variant="caption"
                                    sx={{ mt: 2, mb: 1, display: "block" }}
                                    color="text.secondary"
                                 >
                                    GCS Credentials
                                 </Typography>
                                 {gcsAttachedDatabaseFields.map((field) => (
                                    <TextField
                                       key={`gcs_${field.name}`}
                                       margin="dense"
                                       id={`ducklake_gcs_${field.name}`}
                                       name={`ducklake_gcs_${field.name}`}
                                       label={field.label}
                                       type={field.type}
                                       fullWidth
                                       variant="standard"
                                       required={
                                          field.required &&
                                          field.name !== "secret"
                                       }
                                       defaultValue={getDucklakeDefault(
                                          `gcs_${field.name}`,
                                       )}
                                       placeholder={
                                          field.name === "secret"
                                             ? "Leave empty to keep existing"
                                             : undefined
                                       }
                                    />
                                 ))}
                              </>
                           )}
                        </Box>
                     </Box>
                  ) : type === "duckdb" ? (
                     <Box sx={{ mt: 2 }}>
                        <Box
                           sx={{
                              display: "flex",
                              justifyContent: "space-between",
                              alignItems: "center",
                              mb: 2,
                           }}
                        >
                           <Typography variant="subtitle1" fontWeight={500}>
                              Attached Databases
                           </Typography>
                           <Button
                              startIcon={<AddIcon />}
                              onClick={addAttachedDatabase}
                              size="small"
                              variant="outlined"
                           >
                              Add Database
                           </Button>
                        </Box>
                        {attachedDatabases.length === 0 && (
                           <Typography
                              variant="body2"
                              color="text.secondary"
                              sx={{ mb: 2 }}
                           >
                              DuckDB connections require at least one attached
                              database. Click &quot;Add Database&quot; to get
                              started.
                           </Typography>
                        )}
                        {attachedDatabases.map((db, index) => {
                           const dbFields = getAttachedDatabaseFields(
                              db.dbType,
                           );
                           return (
                              <Box
                                 key={index}
                                 sx={{
                                    mb: 3,
                                    p: 2,
                                    border: "1px solid",
                                    borderColor: "divider",
                                    borderRadius: 1,
                                 }}
                              >
                                 <Box
                                    sx={{
                                       display: "flex",
                                       justifyContent: "space-between",
                                       alignItems: "center",
                                       mb: 2,
                                    }}
                                 >
                                    <Typography variant="subtitle2">
                                       Database {index + 1}
                                    </Typography>
                                    <IconButton
                                       aria-label={`Remove attached database ${index + 1}`}
                                       onClick={() =>
                                          removeAttachedDatabase(index)
                                       }
                                       size="small"
                                       color="error"
                                    >
                                       <DeleteIcon fontSize="small" />
                                    </IconButton>
                                 </Box>
                                 <TextField
                                    margin="dense"
                                    required
                                    id={`attachedDb_${index}_name`}
                                    name={`attachedDb_${index}_name`}
                                    label="Database Name"
                                    type="text"
                                    fullWidth
                                    variant="standard"
                                    defaultValue={db.name}
                                 />
                                 <TextField
                                    margin="dense"
                                    id={`attachedDb_${index}_type`}
                                    name={`attachedDb_${index}_type`}
                                    label="Database Type"
                                    fullWidth
                                    variant="standard"
                                    value={db.dbType}
                                    select
                                    onChange={(event) =>
                                       updateAttachedDatabaseType(
                                          index,
                                          event.target
                                             .value as AttachedDatabaseTypeEnum,
                                       )
                                    }
                                 >
                                    {Object.values(
                                       AttachedDatabaseTypeEnum,
                                    ).map((dbType) => (
                                       <MenuItem key={dbType} value={dbType}>
                                          {dbType}
                                       </MenuItem>
                                    ))}
                                 </TextField>
                                 {dbFields
                                    .filter((field) => {
                                       if (!field.visibleWhen) return true;
                                       const currentValue =
                                          db[field.visibleWhen.field] ||
                                          getAttachedDbDefault(
                                             index,
                                             field.visibleWhen.field,
                                          ) ||
                                          dbFields.find(
                                             (f) =>
                                                f.name ===
                                                field.visibleWhen?.field,
                                          )?.selectOptions?.[0]?.value;
                                       return (
                                          currentValue ===
                                          field.visibleWhen.value
                                       );
                                    })
                                    .map((field) => (
                                       <TextField
                                          key={field.name}
                                          margin="dense"
                                          id={`attachedDb_${index}_${field.name}`}
                                          name={`attachedDb_${index}_${field.name}`}
                                          label={field.label}
                                          type={
                                             field.selectOptions
                                                ? undefined
                                                : field.type
                                          }
                                          fullWidth
                                          variant="standard"
                                          required={field.required}
                                          select={!!field.selectOptions}
                                          defaultValue={
                                             getAttachedDbDefault(
                                                index,
                                                field.name,
                                             ) ||
                                             field.selectOptions?.[0]?.value
                                          }
                                          onChange={
                                             field.selectOptions
                                                ? (e) => {
                                                     const updated = [
                                                        ...attachedDatabases,
                                                     ];
                                                     updated[index] = {
                                                        ...updated[index],
                                                        [field.name]:
                                                           e.target.value,
                                                     };
                                                     setAttachedDatabases(
                                                        updated,
                                                     );
                                                  }
                                                : undefined
                                          }
                                       >
                                          {field.selectOptions?.map(
                                             (option) => (
                                                <MenuItem
                                                   key={option.value}
                                                   value={option.value}
                                                >
                                                   {option.label}
                                                </MenuItem>
                                             ),
                                          )}
                                       </TextField>
                                    ))}
                              </Box>
                           );
                        })}
                     </Box>
                  ) : (
                     connectionFieldsByType[type].map((field) => {
                        const existingValue =
                           connection?.[attributesFieldName[type] ?? ""]?.[
                              field.name ?? ""
                           ] ?? "";
                        const isPasswordField =
                           field.type === "password" ||
                           field.name === "password" ||
                           field.name === "secretAccessKey" ||
                           field.name === "secret" ||
                           field.name === "accessToken" ||
                           field.name === "privateKey";
                        return (
                           <TextField
                              key={field.name}
                              margin="dense"
                              id={field.name}
                              name={field.name}
                              label={field.label}
                              type={field.type}
                              fullWidth
                              variant="standard"
                              required={
                                 field.required &&
                                 !isPasswordField &&
                                 !existingValue
                              }
                              defaultValue={existingValue}
                              placeholder={
                                 isPasswordField && existingValue
                                    ? "Leave empty to keep existing"
                                    : undefined
                              }
                           />
                        );
                     })
                  )}
               </form>
            </DialogContent>
            <DialogActions
               onClick={(event) => {
                  event.stopPropagation();
               }}
            >
               <Button disabled={isSubmitting} onClick={handleClose}>
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="connection-form"
                  loading={isSubmitting}
               >
                  Edit Connection
               </Button>
            </DialogActions>
         </Dialog>
      </React.Fragment>
   );
}
