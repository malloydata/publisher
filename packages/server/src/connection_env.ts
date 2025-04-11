import path from "path";
import { fileURLToPath } from "url";
import { ConnectionDto } from "./dto";

const ROOT_DIR = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const ENV_DIR = path.join(ROOT_DIR, "env");

export async function getEnvConfig(
   currentEnv: string | undefined = Bun.env.PUBLISHER_ENV,
): Promise<Array<ConnectionDto>> {
   console.log("================================");
   console.log(ENV_DIR);
   console.log("================================");

   if (!currentEnv) {
      return [];
   }

   // const envConfig = await getEnvConfig();
   // return envConfig;
   return [];
}
