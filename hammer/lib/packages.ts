// Writes a generated Malloy package to disk (publisher.json + model files) for
// the server to load from its config `location`.

import path from "path";

export interface ModelFile {
   /** Relative path within the package, e.g. "model.malloy". */
   path: string;
   text: string;
}

export async function writePackage(
   root: string,
   spec: {
      name: string;
      version?: string;
      description?: string;
      models: ModelFile[];
   },
): Promise<string> {
   const dir = path.join(root, spec.name);
   await Bun.write(
      path.join(dir, "publisher.json"),
      JSON.stringify(
         {
            name: spec.name,
            version: spec.version ?? "1.0.0",
            description: spec.description ?? `hammer package ${spec.name}`,
         },
         null,
         2,
      ),
   );
   for (const m of spec.models) {
      await Bun.write(path.join(dir, m.path), m.text);
   }
   return dir;
}
