// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as React from "react";
import dayjs, { Dayjs } from "dayjs";
import Button from "@mui/material/Button";
import CalendarTodayRoundedIcon from "@mui/icons-material/CalendarTodayRounded";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";

interface ButtonFieldProps {
  setOpen?: React.Dispatch<React.SetStateAction<boolean>>;
  label?: string;
  disabled?: boolean;
}

function ButtonField(props: ButtonFieldProps) {
  const { setOpen, label, disabled = false } = props;

  return (
    <Button
      variant="outlined"
      disabled={disabled}
      size="small"
      onClick={() => setOpen?.((prev) => !prev)}
      startIcon={<CalendarTodayRoundedIcon fontSize="small" />}
      sx={{ minWidth: "fit-content" }}
    >
      {label ? `${label}` : "Pick a date"}
    </Button>
  );
}

export default function CustomDatePicker() {
  const [value, setValue] = React.useState<Dayjs | null>(dayjs("2023-04-17"));
  const [open, setOpen] = React.useState(false);

  return (
    <LocalizationProvider dateAdapter={AdapterDayjs}>
      <DatePicker
        value={value}
        label={value == null ? null : value.format("MMM DD, YYYY")}
        onChange={(newValue) => setValue(newValue)}
        slots={{ field: ButtonField }}
        slotProps={{
          field: { setOpen } as any,
          nextIconButton: { size: "small" },
          previousIconButton: { size: "small" },
        }}
        open={open}
        onClose={() => setOpen(false)}
        onOpen={() => setOpen(true)}
        views={["day", "month", "year"]}
      />
    </LocalizationProvider>
  );
}
