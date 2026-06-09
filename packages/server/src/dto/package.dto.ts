import {
   IsString,
   IsNotEmpty,
   IsOptional,
   IsArray,
   IsIn,
} from "class-validator";
import { ApiPackage } from "../service/package";

export class PackageDto implements ApiPackage {
   @IsString()
   @IsNotEmpty()
   name: string;

   @IsString()
   @IsNotEmpty()
   description: string;

   @IsOptional()
   @IsArray()
   @IsString({ each: true })
   explores?: string[];

   @IsOptional()
   @IsIn(["declared", "all"])
   queryableSources?: "declared" | "all";
}
