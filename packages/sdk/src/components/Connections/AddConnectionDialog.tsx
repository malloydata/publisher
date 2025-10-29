import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import React, { useState } from "react";

import MenuItem from "@mui/material/MenuItem";
import { Connection, ConnectionTypeEnum } from "../../client/api";
import { connectionFieldsByType } from "./common";

type AddConnectionDialogProps = {
   onSubmit: (connection: Connection) => Promise<unknown>;
   isSubmitting: boolean;
};

export default function AddConnectionDialog({
   onSubmit,
   isSubmitting,
}: AddConnectionDialogProps) {
   const [open, setOpen] = useState(false);

   const [type, setType] = useState<Connection["type"]>("postgres");
   const handleClickOpen = () => {
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
      const attributesFieldName = {
         postgres: "postgresConnection",
         bigquery: "bigqueryConnection",
         snowflake: "snowflakeConnection",
         trino: "trinoConnection",
         mysql: "mysqlConnection",
         motherduck: "motherduckConnection",
      };
      const connectionPayload = {
         name,
         type,
         [attributesFieldName[type]]: fields.reduce((acc, field) => {
            acc[field.name] = formData.get(field.name)?.toString();
            return acc;
         }, {}),
      } satisfies Connection;
      await onSubmit(connectionPayload);
      handleClose();
   };

   return (
      <React.Fragment>
         <Button variant="outlined" onClick={handleClickOpen}>
            Add Connection
         </Button>
         <Dialog open={open} onClose={handleClose}>
            <DialogTitle>Create New Connection</DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Add a new connection to query your data database using Malloy.
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
                  {connectionFieldsByType[type].map((field) => (
                     <TextField
                        key={field.name}
                        margin="dense"
                        id={field.name}
                        name={field.name}
                        label={field.label}
                        type={field.type}
                        fullWidth
                        variant="standard"
                     />
                  ))}
               </form>
            </DialogContent>
            <DialogActions>
               <Button disabled={isSubmitting} onClick={handleClose}>
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="connection-form"
                  loading={isSubmitting}
               >
                  Create Connection
               </Button>
            </DialogActions>
         </Dialog>
      </React.Fragment>
   );
}
