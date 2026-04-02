import { TaskService } from "../service/task_service";
import { TaskConfig } from "../storage/DatabaseInterface";

export class TaskController {
   constructor(private taskService: TaskService) {}

   async listTasks(projectName: string) {
      return this.taskService.listTasks(projectName);
   }

   async getTask(projectName: string, taskId: string) {
      return this.taskService.getTask(projectName, taskId);
   }

   async createTask(
      projectName: string,
      body: { name: string; type?: string; config: TaskConfig },
   ) {
      return this.taskService.createTask(projectName, body);
   }

   async updateTask(
      projectName: string,
      taskId: string,
      body: Partial<{ name: string; type: string; config: TaskConfig }>,
   ) {
      return this.taskService.updateTask(projectName, taskId, body);
   }

   async deleteTask(projectName: string, taskId: string) {
      return this.taskService.deleteTask(projectName, taskId);
   }

   async startTask(
      projectName: string,
      taskId: string,
      body: { autoLoadManifest?: boolean; forceRefresh?: boolean },
   ) {
      return this.taskService.startTask(projectName, taskId, body);
   }

   async stopTask(projectName: string, taskId: string) {
      return this.taskService.stopTask(projectName, taskId);
   }

   async getTaskStatus(projectName: string, taskId: string) {
      return this.taskService.getTaskStatus(projectName, taskId);
   }

   async listExecutions(projectName: string, taskId: string) {
      return this.taskService.listExecutions(projectName, taskId);
   }

   async getExecution(
      projectName: string,
      taskId: string,
      executionId: string,
   ) {
      return this.taskService.getExecution(projectName, taskId, executionId);
   }
}
