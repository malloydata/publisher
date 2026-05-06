import { describe, expect, it } from "bun:test";
import {
   generateEnvironmentReadme,
   getEnvironmentDescription,
} from "./parsing";

describe("getEnvironmentDescription", () => {
   it("should return the first paragraph of the README", () => {
      const readme =
         "# Environment Description\nThis is an environment description";
      expect(getEnvironmentDescription(readme)).toBe(
         "Environment Description\nThis is an environment description",
      );
   });

   it("should return a truncated description if it is longer than 120 characters", () => {
      const longDescription = Array(40).fill("abcde").join(" ");
      const readme = `${longDescription}`;
      // 5 characters per word + space, so 20 words = 120 characters
      const truncatedDescription = Array(20).fill("abcde").join(" ") + "...";
      expect(getEnvironmentDescription(readme)).toBe(truncatedDescription);
   });

   it("should return a placeholder description if the README is empty", () => {
      const readme = "";
      expect(getEnvironmentDescription(readme)).toBe(
         "Explore semantic models, run queries, and build dashboards",
      );
   });
});

describe("generateEnvironmentReadme", () => {
   it("should preserve the existing readme if it exists", () => {
      const environment = {
         name: "Test Environment",
         readme: "# Test Readme",
      };
      expect(generateEnvironmentReadme(environment)).toBe("# Test Readme");
   });

   it("should generate an environment readme with the description if it does not exist", () => {
      const environment = {
         name: "Test Environment",
         readme: "",
      };
      expect(generateEnvironmentReadme(environment, "Test Description")).toBe(
         "# Test Environment\n\nTest Description",
      );
   });

   it("should insert the description in the existing readme if both exist", () => {
      const environment = {
         name: "Test Environment",
         readme: "# Test Readme\n\nOld Description\n\nMore stuff",
      };
      expect(generateEnvironmentReadme(environment, "New Description")).toBe(
         "# Test Readme\n\nNew Description\n\nMore stuff",
      );
   });
});
