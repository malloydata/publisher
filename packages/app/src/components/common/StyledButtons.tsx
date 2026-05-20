import { Button, ButtonProps, Chip, ChipProps, styled } from "@mui/material";
import { forwardRef, ReactElement } from "react";

const StyledPillButton = styled(Button)<ButtonProps>(
   ({ theme, variant, color }) => ({
      borderRadius: "20px",
      textTransform: "none",
      fontWeight: 500,
      paddingLeft: theme.spacing(2),
      paddingRight: theme.spacing(2),
      paddingTop: theme.spacing(0.75),
      paddingBottom: theme.spacing(0.75),
      boxShadow: "none",
      fontSize: "0.875rem",
      minWidth: "fit-content",
      flexShrink: 0,
      overflow: "visible",
      "& .MuiButton-startIcon": {
         marginRight: theme.spacing(1),
         marginLeft: 0,
         overflow: "visible",
      },
      "& .MuiButton-endIcon": {
         marginLeft: theme.spacing(1),
         marginRight: 0,
         overflow: "visible",
      },
      ...(!variant || variant === "text"
         ? {
              color: theme.palette.primary.main,
              "&:hover": {
                 backgroundColor: `${theme.palette.primary.main}08`,
              },
           }
         : {}),
      ...(variant === "contained" &&
         (!color || color === "primary") && {
            backgroundColor: theme.palette.primary.main,
            color: theme.palette.primary.contrastText,
            "&:hover": {
               backgroundColor: theme.palette.grey[800],
               boxShadow: "none",
            },
            "&:disabled": {
               backgroundColor: theme.palette.grey[300],
               color: theme.palette.grey[500],
            },
         }),
      ...(variant === "outlined" &&
         (!color || color === "primary") && {
            borderColor: theme.palette.primary.main,
            color: theme.palette.primary.main,
            "&:hover": {
               borderColor: theme.palette.grey[800],
               backgroundColor: `${theme.palette.primary.main}10`,
            },
            "&:disabled": {
               borderColor: theme.palette.grey[300],
               color: theme.palette.grey[400],
            },
         }),
      ...(color === "error" &&
         variant === "outlined" && {
            borderColor: theme.palette.error.main,
            color: theme.palette.error.main,
            "&:hover": {
               borderColor: theme.palette.error.dark,
               backgroundColor: theme.palette.error.light + "10",
            },
         }),
      ...(color === "error" &&
         variant === "contained" && {
            backgroundColor: theme.palette.error.main,
            "&:hover": {
               backgroundColor: theme.palette.error.dark,
               boxShadow: "none",
            },
         }),
   }),
);

/**
 * Rounded square button with consistent styling
 * Default variant is "outlined"
 */
export const PillButton = forwardRef<HTMLButtonElement, ButtonProps>(
   ({ variant = "outlined", ...props }, ref) => (
      <StyledPillButton ref={ref} variant={variant} {...props} />
   ),
);
PillButton.displayName = "PillButton";

interface StyledButtonProps extends Omit<ButtonProps, "variant"> {
   icon?: ReactElement;
   to?: string;
}

/**
 * Secondary button with outlined styling
 */
export function SecondaryButton({
   icon,
   children,
   ...props
}: StyledButtonProps) {
   return (
      <PillButton startIcon={icon} {...props}>
         {children}
      </PillButton>
   );
}

/**
 * Primary button
 */
export function PrimaryButton({ icon, children, ...props }: StyledButtonProps) {
   return (
      <PillButton variant="contained" startIcon={icon} {...props}>
         {children}
      </PillButton>
   );
}

/**
 * Styled chip for displaying type indicators (e.g., "User", "Group")
 */
export const TypeChip = styled(Chip)<ChipProps>(() => ({
   borderRadius: "6px",
   textTransform: "none",
   fontWeight: 400,
   fontSize: "0.875rem",
   height: 28,
   "& .MuiChip-icon": {
      fontSize: 16,
      marginLeft: 8,
   },
   "& .MuiChip-label": {
      paddingLeft: 8,
      paddingRight: 8,
   },
}));

interface SelectableChipProps extends Omit<ChipProps, "onClick"> {
   selected?: boolean;
   onClick?: () => void;
}

/**
 * Selectable chip for toggle options (e.g., Users/Groups selector)
 */
export function SelectableChip({
   selected,
   onClick,
   ...props
}: SelectableChipProps) {
   return (
      <Chip
         {...props}
         onClick={onClick}
         role="button"
         aria-pressed={selected}
         color={selected ? "primary" : "default"}
         variant={selected ? "filled" : "outlined"}
         sx={{
            cursor: "pointer",
            borderRadius: "8px",
            fontWeight: 500,
            "& .MuiChip-icon": {
               fontSize: 18,
            },
            ...(selected && {
               bgcolor: "primary.main",
               "&:hover": {
                  bgcolor: "grey.800",
               },
            }),
            ...(!selected && {
               borderColor: "divider",
               "&:hover": {
                  bgcolor: "action.hover",
               },
            }),
            ...props.sx,
         }}
      />
   );
}

/**
 * Small badge chip (e.g., "You" indicator)
 */
export const BadgeChip = styled(Chip)<ChipProps>(({ theme }) => ({
   height: 20,
   fontSize: 11,
   fontWeight: 500,
   backgroundColor: theme.palette.background.default,
   color: theme.palette.text.primary,
   borderRadius: "4px",
   "& .MuiChip-label": {
      paddingLeft: 6,
      paddingRight: 6,
   },
}));

const StyledLatestChip = styled(Chip)<ChipProps>(({ theme }) => ({
   height: 20,
   fontSize: "0.625rem",
   fontWeight: 600,
   backgroundColor: theme.palette.info.light,
   color: theme.palette.info.dark,
   "& .MuiChip-label": {
      paddingLeft: theme.spacing(0.75),
      paddingRight: theme.spacing(0.75),
   },
}));

/**
 * Chip marking the latest/pinned version of a resource
 */
export function LatestChip(props: Omit<ChipProps, "label">) {
   return <StyledLatestChip label="LATEST" size="small" {...props} />;
}
