import { Repository } from 'typeorm';
import { Task } from '../models/Task';
import { getJobForTaskType } from '../jobs/JobFactory';
import { WorkflowStatus } from '../workflows/WorkflowFactory';
import { Workflow } from '../models/Workflow';
import { Result } from '../models/Result';

export enum TaskStatus {
    Queued = 'queued',
    InProgress = 'in_progress',
    Completed = 'completed',
    Failed = 'failed'
}

export class TaskRunner {
    constructor(
        private taskRepository: Repository<Task>,
    ) {}

    /**
     * Runs the appropriate job based on the task's type, managing the task's status.
     * @param task - The task entity that determines which job to run.
     * @throws If the job fails, it rethrows the error.
     */
    async run(task: Task): Promise<void> {
        const taskRepository = this.taskRepository.manager.getRepository(Task);
        
        const dependentTask = await taskRepository.findOne({
            where: { taskId: task.taskId },
            relations: ['dependsOn']
        });

        if (dependentTask?.dependsOn && dependentTask.dependsOn.status !== TaskStatus.Completed) {
            console.log(`Task ${task.taskId} depends on task ${dependentTask.dependsOn.taskId}. Waiting for dependent task to complete...`);
            return; // Wait for the dependent task to complete
        }

        task.status = TaskStatus.InProgress;
        task.progress = 'starting job...';
        await this.taskRepository.save(task);

        const job = getJobForTaskType(task.taskType);

        try {
            console.log(`Starting job ${task.taskType} for task ${task.taskId}...`);
            const resultRepository = this.taskRepository.manager.getRepository(Result);
            const taskResult = await job.run(task);
            console.log(`Job ${task.taskType} for task ${task.taskId} completed successfully.`);

            const result = new Result();
            result.taskId = task.taskId!;
            result.data = JSON.stringify(taskResult || {});
            await resultRepository.save(result);

            task.resultId = result.resultId!;
            task.status = TaskStatus.Completed;
            task.progress = null;
            await this.taskRepository.save(task);

        } catch (error: any) {
            console.error(`Error running job ${task.taskType} for task ${task.taskId}:`, error);

            task.status = TaskStatus.Failed;
            task.progress = null;
            await this.taskRepository.save(task);

            throw error;
        }

        const workflowRepository = this.taskRepository.manager.getRepository(Workflow);
        const currentWorkflow = await workflowRepository.findOne({ where: { workflowId: task?.workflow?.workflowId }, relations: ['tasks'] });

        if (currentWorkflow) {
            const workflowTasks = currentWorkflow.tasks;
            const allCompleted = workflowTasks.every(t => t.status === TaskStatus.Completed);
            const anyFailed = workflowTasks.some(t => t.status === TaskStatus.Failed);

            if (anyFailed) {
                currentWorkflow.status = WorkflowStatus.Failed;
            } else if (allCompleted) {
                currentWorkflow.status = WorkflowStatus.Completed;
            } else {
                currentWorkflow.status = WorkflowStatus.InProgress;
            }

            // Aggregate the results of all tasks
            const aggregatedResults = workflowTasks.map(t => ({
                taskId: t.taskId,
                type: t.taskType,
                output: t.output || null,
                status: t.status
            }));

            currentWorkflow.finalResult = aggregatedResults;
            await workflowRepository.save(currentWorkflow);

            // Execute the next task in the sequence
            const nextTask = workflowTasks.find(t => t.stepNumber === task.stepNumber + 1 && t.status === TaskStatus.Queued);
            if (nextTask) {
                await this.run(nextTask);
            }
        }
    }

    /**
     * Starts the workflow by running the first task.
     * @param workflow - The workflow entity to start.
     */
    async startWorkflow(workflow: Workflow): Promise<void> {
        const firstTask = workflow.tasks.find(t => t.stepNumber === 1 && t.dependsOn === null);
        if (firstTask) {
            await this.run(firstTask);
        } else {
            console.error('No starting task found for the workflow.');
        }
    }
}