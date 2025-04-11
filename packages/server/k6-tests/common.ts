import http from "k6/http";
import { check } from "k6";

type ApiPackage = {
   name: string;
   description: string;
};

const samples = {
   auto_recalls: open("./packages/auto_recalls.zip", "b"),
   ecommerce: open("./packages/ecommerce.zip", "b"),
   faa: open("./packages/faa.zip", "b"),
   ga4: open("./packages/ga4.zip", "b"),
   imdb: open("./packages/imdb.zip", "b"),
   names: open("./packages/names.zip", "b"),
   patterns: open("./packages/patterns.zip", "b"),
   bq_ga4: open("./packages/bq_ga4.zip", "b"),
   bq_ga_sessions: open("./packages/bq_ga_sessions.zip", "b"),
   bq_hackernews: open("./packages/bq_hackernews.zip", "b"),
   bq_the_met: open("./packages/bq_the_met.zip", "b"),
};
type SampleName = keyof typeof samples;
export const sampleNames = Object.keys(samples) as SampleName[];

const PUBLISHER_URL = __ENV.PUBLISHER_URL || "http://localhost:4000";

export const getPackages = () => {
   const response = http.get(`${PUBLISHER_URL}/api/v0/projects/home/packages`);
   return response.json() as Array<{
      name: SampleName;
      description: string;
   }>;
};

export const loadPackage = (packageName: SampleName) => {
   const apiPackage: ApiPackage = {
      name: packageName,
      description: "",
   };
   const formData = {
      package: JSON.stringify(apiPackage),
      connectionList: JSON.stringify({
         connections: [
            {
               connectionConfigJson: JSON.stringify({}),
            },
         ],
      }),
      packageFile: http.file(samples[packageName], packageName + ".zip"),
   };

   const response = http.post(`${PUBLISHER_URL}/packages`, formData);

   check(response, {
      [`package ${packageName} can upload`]: (r) => r.status === 200,
      [`package ${packageName} upload time < 500ms`]: (r) =>
         r.timings.duration < 500,
   });

   return apiPackage;
};

export const queryPackage = (packageName: SampleName) => {
   const response = http.get(
      `${PUBLISHER_URL}/api/v0/projects/home/packages/${packageName}`,
   );

   check(response, {
      "package query successful": (r) => r.status === 200,
      "package query response time < 500ms": (r) => r.timings.duration < 500,
   });

   return response.json() as ApiPackage;
};

export const unloadPackage = (packageName: SampleName, packageId: string) => {
   const response = http.del(`${PUBLISHER_URL}/packages/${packageId}`);

   check(response, {
      [`package ${packageName} can unload`]: (r) => r.status === 204,
      [`package ${packageName} unload time < 500ms`]: (r) =>
         r.timings.duration < 500,
   });
};

export const getModels = (packageName: SampleName) => {
   const encodedPackageName = encodeURIComponent(packageName);
   const response = http.get(
      `${PUBLISHER_URL}/api/v0/projects/home/packages/${encodedPackageName}/models`,
   );
   return response.json() as Array<{
      path: string;
      type: string;
   }>;
};

type ModelData = {
   type: "notebook" | "source";
   sources?: Array<{
      name: string;
      views: Array<{
         name: string;
      }>;
   }>;
};

export const getModelData = (packageName: SampleName, modelPath: string) => {
   const encodedPackageName = encodeURIComponent(packageName);
   const encodedModelPath = encodeURIComponent(modelPath);
   const modelResponse = http.get(
      `${PUBLISHER_URL}/api/v0/projects/home/packages/${encodedPackageName}/models/${encodedModelPath}`,
   );

   return modelResponse.json() as ModelData;
};

export function* getViews(modelData: ModelData) {
   if (modelData.type === "source" && modelData.sources) {
      for (const source of modelData.sources) {
         for (const view of source.views) {
            yield {
               sourceName: source.name,
               viewName: view.name,
            };
         }
      }
   }
}

export const queryModelView = (
   packageName: SampleName,
   modelPath: string,
   sourceName: string,
   queryName: string,
) => {
   const encodedPackageName = encodeURIComponent(packageName);
   const encodedModelPath = encodeURIComponent(modelPath);
   const encodedSourceName = encodeURIComponent(sourceName);
   const encodedQueryName = encodeURIComponent(queryName);
   return http.get(
      `${PUBLISHER_URL}/api/v0/projects/home/packages/${encodedPackageName}/queryResults/${encodedModelPath}?sourceName=${encodedSourceName}&queryName=${encodedQueryName}`,
   );
};
