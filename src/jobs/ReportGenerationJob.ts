import { Job } from "./Job";
import { Task } from "../models/Task";
import { AppDataSource } from "../data-source";
import { Workflow } from "../models/Workflow";
import { TaskStatus } from "../workers/taskRunner";

export class ReportGenerationJob implements Job {
  async run(task: Task): Promise<void> {
    const taskRepository = AppDataSource.getRepository(Task);
    const workflowRepository = AppDataSource.getRepository(Workflow);

    // Fetch the task with its workflow
    const fetchedTask = await taskRepository.findOne({
      where: { taskId: task.taskId },
      relations: ['workflow']
    });

    if (!fetchedTask) {
      throw new Error(`Task not found: ${task.taskId}`);
    }

    const workflow = await workflowRepository.findOne({
      where: { workflowId: fetchedTask.workflow.workflowId },
      relations: ['tasks']
    });

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

    await taskRepository.save(task);
  }
}
