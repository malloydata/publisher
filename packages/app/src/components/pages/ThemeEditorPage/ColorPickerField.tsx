import { Box, Popover, Stack, TextField, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { HexColorPicker } from "react-colorful";

interface ColorPickerFieldProps {
   value: string;
   onChange: (hex: string) => void;
   label?: string;
   disabled?: boolean;
}

const HEX_REGEX = /^#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

/**
 * Reusable color input: clickable swatch that opens a react-colorful
 * popover + a text field for hex entry. The text input only commits its
 * value on blur (or Enter) and rejects malformed hex strings so the
 * draft theme never holds an invalid value.
 */
export function ColorPickerField({
   value,
   onChange,
   label,
   disabled,
}: ColorPickerFieldProps) {
   const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
   const [text, setText] = useState(value);

   // Keep the text field in sync when the value changes externally
   // (e.g. another section's reset). Done in an effect so the
   // document.activeElement check runs after commit, not during render
   // where it would fire under SSR/StrictMode and risk a render loop.
   useEffect(() => {
      if (text !== value && document.activeElement?.tagName !== "INPUT") {
         setText(value);
      }
      // text is intentionally excluded — we only want to react to external
      // value changes, not to our own setText.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [value]);

   const commitText = () => {
      if (HEX_REGEX.test(text)) {
         if (text !== value) onChange(text);
      } else {
         // Invalid input — revert the field to the committed value.
         setText(value);
      }
   };

   return (
      <Stack spacing={0.5}>
         {label && (
            <Typography
               variant="caption"
               color="text.secondary"
               component="label"
               sx={{ fontSize: "0.75rem", lineHeight: 1.4 }}
            >
               {label}
            </Typography>
         )}
         <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Box
               role="button"
               aria-label={label ? `Pick ${label}` : "Pick color"}
               tabIndex={disabled ? -1 : 0}
               onClick={(e) => !disabled && setAnchorEl(e.currentTarget)}
               onKeyDown={(e) => {
                  if (disabled) return;
                  if (e.key === "Enter" || e.key === " ") {
                     e.preventDefault();
                     setAnchorEl(e.currentTarget);
                  }
               }}
               sx={(theme) => ({
                  width: 32,
                  height: 32,
                  borderRadius: 1,
                  backgroundColor: value,
                  border: `1px solid ${theme.palette.divider}`,
                  cursor: disabled ? "not-allowed" : "pointer",
                  opacity: disabled ? 0.5 : 1,
               })}
            />
            <TextField
               size="small"
               value={text}
               disabled={disabled}
               onChange={(e) => setText(e.target.value)}
               onBlur={commitText}
               onKeyDown={(e) => {
                  if (e.key === "Enter") {
                     commitText();
                     (e.target as HTMLInputElement).blur();
                  } else if (e.key === "Escape") {
                     setText(value);
                     (e.target as HTMLInputElement).blur();
                  }
               }}
               inputProps={{ maxLength: 7, "aria-label": label }}
               sx={{ width: 110 }}
            />
         </Box>
         <Popover
            open={!!anchorEl}
            anchorEl={anchorEl}
            onClose={() => setAnchorEl(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
         >
            <Box sx={{ p: 1.5 }}>
               <HexColorPicker
                  color={value}
                  onChange={(c) => {
                     setText(c);
                     onChange(c);
                  }}
               />
            </Box>
         </Popover>
      </Stack>
   );
}
