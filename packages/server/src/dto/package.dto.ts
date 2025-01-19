import { IsString, IsNotEmpty } from "class-validator";

export class PackageDto {
   @IsString()
   @IsNotEmpty()
   name: string;

   @IsString()
   @IsNotEmpty()
   description: string;
}
