import { expect, it, describe } from "bun:test";
import request from "supertest";
import express from "express";
import { validateSchema } from "./validate";
import { PackageDto } from "./package.dto";
import { faker } from "@faker-js/faker";

const app = express();
app.use(express.json());

app.post("/packages", validateSchema(PackageDto), (req, res) => {
   res.status(201).json({ message: "Package created", data: req.body });
});

describe("dto/validate", () => {
   describe("validateSchema", () => {
      it("should pass validation for a valid request", async () => {
         const dummyPayload = {
            name: faker.person.firstName(),
            description: faker.person.lastName(),
         };

         const response = await request(app)
            .post("/packages")
            .send(dummyPayload);

         expect(response.status).toBe(201);
         expect(response.body.message).toBe("Package created");
         expect(response.body.data).toEqual(dummyPayload);
      });

      it("should return 400 for missing required fields", async () => {
         const response = await request(app).post("/packages").send({
            name: "",
            description: "",
         });

         expect(response.status).toBe(400);
         expect(Array.isArray(response.body.errors)).toBe(true);

         const expectedErrors = [
            {
               property: "name",
               constraints: { isNotEmpty: "name should not be empty" },
            },
            {
               property: "description",
               constraints: {
                  isNotEmpty: "description should not be empty",
               },
            },
         ];
         expect(response.body.errors).toEqual(
            expect.arrayContaining(expectedErrors),
         );
      });

      it("should return 400 for invalid description format", async () => {
         const response = await request(app).post("/packages").send({
            name: faker.person.firstName(),
            description: false,
         });

         expect(response.status).toBe(400);
         const expectedErrors = [
            {
               property: "description",
               constraints: { isString: "description must be a string" },
            },
         ];
         expect(response.body.errors).toEqual(
            expect.arrayContaining(expectedErrors),
         );
      });
   });
});
