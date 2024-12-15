import { ReportGenerationJob } from '../../src/jobs/ReportGenerationJob';
import { Task } from '../../src/models/Task';
import { Workflow } from '../../src/models/Workflow';
import { TaskStatus } from '../../src/workers/taskRunner';
import { AppDataSource } from '../../src/data-source';
import { Repository } from 'typeorm';

jest.mock('../../src/data-source', () => {
    const originalModule = jest.requireActual('../../src/data-source');
    return {
        ...originalModule,
        AppDataSource: {
            ...originalModule.AppDataSource,
            initialize: jest.fn().mockResolvedValue(undefined),
            destroy: jest.fn().mockResolvedValue(undefined),
            getRepository: jest.fn(),
        },
    };
});

describe('ReportGenerationJob', () => {
    let job: ReportGenerationJob;
    let task: Task;
    let workflow: Workflow;
    let workflowRepository: jest.Mocked<Repository<Workflow>>;
    let taskRepository: jest.Mocked<Repository<Task>>;

    beforeAll(async () => {
        await AppDataSource.initialize();
    });

    afterAll(async () => {
        await AppDataSource.destroy();
    });

    beforeEach(() => {
        job = new ReportGenerationJob();
        task = new Task();
        task.taskId = 'report-task-id'; // Set the taskId property
        workflow = new Workflow();
        workflow.workflowId = 'workflow-id';
        workflow.tasks = [
            {
                taskId: 'task-1-id',
                taskType: 'polygonArea',
                output: '{"area": 123.45}',
                status: TaskStatus.Completed,
            } as Task,
            {
                taskId: 'task-2-id',
                taskType: 'dataAnalysis',
                output: '{"result": "analysis result"}',
                status: TaskStatus.Completed,
            } as Task,
        ];
        task.workflow = workflow;
        task.taskType = 'report_generation';

        workflowRepository = {
            findOne: jest.fn(),
            save: jest.fn(),
        } as unknown as jest.Mocked<Repository<Workflow>>;

        taskRepository = {
            findOne: jest.fn(),
            save: jest.fn(),
        } as unknown as jest.Mocked<Repository<Task>>;

        (AppDataSource.getRepository as jest.Mock).mockImplementation((entity) => {
            if (entity === Workflow) {
                return workflowRepository;
            } else if (entity === Task) {
                return taskRepository;
            }
            throw new Error('Unknown entity');
        });
    });

    it('should generate a report by aggregating the outputs of multiple tasks', async () => {
        taskRepository.findOne.mockResolvedValue(task);
        workflowRepository.findOne.mockResolvedValue(workflow);

        await job.run(task);
        
        expect(task.status).toBe(TaskStatus.Completed);
        const output = JSON.parse(task.output);
        expect(output).toHaveProperty('workflowId', 'workflow-id');
        expect(output.tasks).toHaveLength(2);
        expect(output.tasks[0]).toHaveProperty('taskId', 'task-1-id');
        expect(output.tasks[0]).toHaveProperty('type', 'polygonArea');
        expect(output.tasks[0]).toHaveProperty('output', '{"area": 123.45}');
        expect(output.tasks[1]).toHaveProperty('taskId', 'task-2-id');
        expect(output.tasks[1]).toHaveProperty('type', 'dataAnalysis');
        expect(output.tasks[1]).toHaveProperty('output', '{"result": "analysis result"}');
        expect(output).toHaveProperty('finalReport', 'Aggregated data and results');
    });

    it('should mark the task as failed if the workflow is not found', async () => {
        taskRepository.findOne.mockResolvedValue(task);
        workflowRepository.findOne.mockResolvedValue(null);

        await job.run(task);

        expect(task.status).toBe(TaskStatus.Failed);
        const output = JSON.parse(task.output);
        expect(output).toHaveProperty('error', 'Workflow not found');
    });

    it('should mark the task as failed if the task is not found', async () => {
        taskRepository.findOne.mockResolvedValue(null);

        await job.run(task);

        expect(task.status).toBe(TaskStatus.Failed);
        const output = JSON.parse(task.output);
        expect(output).toHaveProperty('error', `Task not found: ${task.taskId}`);
    });
});