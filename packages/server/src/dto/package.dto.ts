import { IsString, IsNotEmpty } from "class-validator";
import { ApiPackage } from "../service/package";

export class PackageDto implements ApiPackage {
   @IsString()
   @IsNotEmpty()
   name: string;

   @IsString()
   @IsNotEmpty()
   description: string;
}
