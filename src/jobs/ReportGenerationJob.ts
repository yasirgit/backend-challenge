import { Job } from "./Job";
import { Task } from "../models/Task";
import { AppDataSource } from "../data-source";
import { Workflow } from "../models/Workflow";
import { TaskStatus } from "../workers/taskRunner";

export class ReportGenerationJob implements Job {
  async run(task: Task): Promise<void> {
    const workflowRepository = AppDataSource.getRepository(Workflow);
    const workflow = await workflowRepository.findOne({ where: { workflowId: task.workflow.workflowId }, relations: ['tasks'] });

    if (!workflow) {
      throw new Error(`Workflow not found for task ${task.taskId}`);
    }

    const repotableTasks = workflow.tasks.filter(t => t.taskType !== "report_generation");

    const report = {
      workflowId: workflow.workflowId,
      tasks: repotableTasks.map(t => ({
        taskId: t.taskId,
        type: t.taskType,
        output: t.output || null,
        error: t.status === TaskStatus.Completed ? t.progress : null
      })),
      finalReport: "Aggregated data and results"
    };

    task.output = report;
    task.status = TaskStatus.Completed;
    task.progress = "100%";
  }
}
