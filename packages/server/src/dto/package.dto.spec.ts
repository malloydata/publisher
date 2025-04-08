import { expect, it, describe } from "bun:test";

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

      expect(errors).toBe.empty;
   });

   it("should throw when no name is specified", async () => {
      const errors = await validate(
         plainToInstance(PackageDto, {
            name: "",
            description: faker.person.lastName(),
         }),
      );

      expect(errors.length).toBeGreaterThan(0);
      expect(errors).toHaveLength(1);
   });

   it("should throw when no description is specified", async () => {
      const errors = await validate(
         plainToInstance(PackageDto, {
            name: faker.person.firstName(),
         }),
      );

      expect(errors.length).toBeGreaterThan(0);
      expect(errors).toHaveLength(1);
   });
});
