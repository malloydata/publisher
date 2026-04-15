import { BuildService } from "../service/build_service";

export class BuildController {
   constructor(private buildService: BuildService) {}

   async startBuild(
      projectName: string,
      packageName: string,
      body: { forceRefresh?: boolean },
   ) {
      return this.buildService.startBuild(projectName, packageName, body);
   }

   async stopBuild(projectName: string, packageName: string) {
      return this.buildService.stopBuild(projectName, packageName);
   }

   async getBuildStatus(projectName: string, packageName: string) {
      return this.buildService.getBuildStatus(projectName, packageName);
   }

   async listExecutions(projectName: string, packageName: string) {
      return this.buildService.listExecutions(projectName, packageName);
   }

   async getExecution(
      projectName: string,
      packageName: string,
      executionId: string,
   ) {
      return this.buildService.getExecution(
         projectName,
         packageName,
         executionId,
      );
   }
}
