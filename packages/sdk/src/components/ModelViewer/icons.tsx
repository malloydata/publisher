// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AbcOutlined from "@mui/icons-material/AbcOutlined";
import ArrowOutward from "@mui/icons-material/ArrowOutward";
import BarChartOutlined from "@mui/icons-material/BarChartOutlined";
import CalculateOutlined from "@mui/icons-material/CalculateOutlined";
import CalendarTodayOutlined from "@mui/icons-material/CalendarTodayOutlined";
import CallMergeOutlined from "@mui/icons-material/CallMergeOutlined";
import CodeOutlined from "@mui/icons-material/CodeOutlined";
import DataArrayOutlined from "@mui/icons-material/DataArrayOutlined";
import DataObjectOutlined from "@mui/icons-material/DataObjectOutlined";
import ExpandLess from "@mui/icons-material/ExpandLess";
import ExpandMore from "@mui/icons-material/ExpandMore";
import FormatListBulletedOutlined from "@mui/icons-material/FormatListBulletedOutlined";
import KeyboardArrowDown from "@mui/icons-material/KeyboardArrowDown";
import LockOutlined from "@mui/icons-material/LockOutlined";
import NumbersOutlined from "@mui/icons-material/NumbersOutlined";
import ScheduleOutlined from "@mui/icons-material/ScheduleOutlined";
import SellOutlined from "@mui/icons-material/SellOutlined";
import ToggleOnOutlined from "@mui/icons-material/ToggleOnOutlined";
import TuneOutlined from "@mui/icons-material/TuneOutlined";
import ViewColumnOutlined from "@mui/icons-material/ViewColumnOutlined";
import WarningAmberOutlined from "@mui/icons-material/WarningAmberOutlined";
import type { SvgIconProps } from "@mui/material";
import type { ComponentType } from "react";

/**
 * The viewer's glyphs, each a Material icon drawn at a pixel size: `size` is
 * the one prop a row passes, so a glyph is the same call whichever icon it is.
 */
export interface IconProps {
   size: number;
   "aria-hidden"?: boolean;
}

export type Icon = ComponentType<IconProps>;

function sized(Glyph: ComponentType<SvgIconProps>, name: string): Icon {
   const Sized = ({ size, ...rest }: IconProps) => (
      <Glyph {...rest} sx={{ fontSize: size }} />
   );
   Sized.displayName = name;
   return Sized;
}

export const IconText = sized(AbcOutlined, "IconText");
export const IconOpen = sized(ArrowOutward, "IconOpen");
export const IconResult = sized(BarChartOutlined, "IconResult");
export const IconCalculation = sized(CalculateOutlined, "IconCalculation");
export const IconDate = sized(CalendarTodayOutlined, "IconDate");
export const IconJoin = sized(CallMergeOutlined, "IconJoin");
export const IconCode = sized(CodeOutlined, "IconCode");
export const IconList = sized(DataArrayOutlined, "IconList");
export const IconRecord = sized(DataObjectOutlined, "IconRecord");
export const IconCollapse = sized(ExpandLess, "IconCollapse");
export const IconExpand = sized(ExpandMore, "IconExpand");
export const IconFields = sized(FormatListBulletedOutlined, "IconFields");
export const IconDropdown = sized(KeyboardArrowDown, "IconDropdown");
export const IconLock = sized(LockOutlined, "IconLock");
export const IconNumber = sized(NumbersOutlined, "IconNumber");
export const IconTimestamp = sized(ScheduleOutlined, "IconTimestamp");
export const IconValues = sized(SellOutlined, "IconValues");
export const IconBoolean = sized(ToggleOnOutlined, "IconBoolean");
export const IconGivens = sized(TuneOutlined, "IconGivens");
export const IconColumn = sized(ViewColumnOutlined, "IconColumn");
export const IconWarning = sized(WarningAmberOutlined, "IconWarning");
