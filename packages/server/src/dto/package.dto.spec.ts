import { expect } from "chai";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { PackageDto } from "./package.dto";
import { faker } from "@faker-js/faker";

describe("dto/package", () => {
   it("should not throw when valid object is provided", async () => {
      const errors = await validate(
         plainToInstance(PackageDto, {
            name: faker.person.firstName(),
            description: faker.person.lastName(),
         }),
      );

      expect(errors).to.be.empty;
   });

   it("should throw when no name is specified", async () => {
      const errors = await validate(
         plainToInstance(PackageDto, {
            name: "",
            description: faker.person.lastName(),
         }),
      );

      expect(errors).to.not.be.empty;
      expect(errors).to.have.lengthOf(1);
   });

   it("should throw when no description is specified", async () => {
      const errors = await validate(
         plainToInstance(PackageDto, {
            name: faker.person.firstName(),
         }),
      );

      expect(errors).to.not.be.empty;
      expect(errors).to.have.lengthOf(1);
   });
});
