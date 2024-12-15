import * as fs from 'fs';
import * as yaml from 'js-yaml';
import { DataSource } from 'typeorm';
import { Workflow } from '../models/Workflow';
import { Task } from '../models/Task';
import { TaskStatus } from '../workers/taskRunner';

export enum WorkflowStatus {
    Initial = 'initial',
    InProgress = 'in_progress',
    Completed = 'completed',
    Failed = 'failed'
}

interface WorkflowStep {
    taskType: string;
    stepNumber: number;
    dependsOn?: number;
}

interface WorkflowDefinition {
    name: string;
    steps: WorkflowStep[];
}

export class WorkflowFactory {
    constructor(private dataSource: DataSource) {}

    async createWorkflowFromYAML(filePath: string, clientId: string, geoJson: string): Promise<Workflow> {
        const workflowDef = this.readYAMLFile(filePath);
        return this.createWorkflow(workflowDef, clientId, geoJson);
    }

    private readYAMLFile(filePath: string): WorkflowDefinition {
        const fileContent = fs.readFileSync(filePath, 'utf8');
        return yaml.load(fileContent) as WorkflowDefinition;
    }

    private async createWorkflow(workflowDef: WorkflowDefinition, clientId: string, geoJson: string): Promise<Workflow> {
        const workflow = await this.createWorkflowEntity(clientId);
        const tasks = this.createTaskEntities(workflowDef, workflow, clientId, geoJson);
        return this.saveWorkflowAndTasks(workflow, tasks);
    }

    private async createWorkflowEntity(clientId: string): Promise<Workflow> {
        const workflowRepository = this.dataSource.getRepository(Workflow);
        const workflow = new Workflow();
        workflow.clientId = clientId;
        workflow.status = WorkflowStatus.Initial;
        return workflowRepository.save(workflow);
    }

    private createTaskEntities(workflowDef: WorkflowDefinition, workflow: Workflow, clientId: string, geoJson: string): Task[] {
        const taskMap: Record<string, Task> = {};
        const tasks: Task[] = [];

        for (const step of workflowDef.steps) {
            const task = new Task();
            task.clientId = clientId;
            task.geoJson = geoJson;
            task.status = TaskStatus.Queued;
            task.taskType = step.taskType;
            task.stepNumber = step.stepNumber;
            task.workflow = workflow;

            if (step.dependsOn) {
                task.dependsOn = taskMap[step.dependsOn];
            } else {
                task.dependsOn = null; // Explicitly set dependsOn to null for the first task
            }

            tasks.push(task);
            taskMap[step.taskType] = task;
        }

        return tasks;
    }

    private async saveWorkflowAndTasks(workflow: Workflow, tasks: Task[]): Promise<Workflow> {
        const workflowRepository = this.dataSource.getRepository(Workflow);
        const taskRepository = this.dataSource.getRepository(Task);

        await taskRepository.save(tasks);

        // Fetch the workflow again with its related tasks
        const workflowWithTasks = await workflowRepository.findOne({
            where: { workflowId: workflow.workflowId },
            relations: ['tasks', 'tasks.dependsOn']
        });

        if (!workflowWithTasks) {
            throw new Error('Failed to fetch the workflow with tasks');
        }

        return workflowWithTasks;
    }
}