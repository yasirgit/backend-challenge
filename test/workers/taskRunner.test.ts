import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { TaskRunner } from '../../src/workers/taskRunner';
import { Task } from '../../src/models/Task';
import { Workflow } from '../../src/models/Workflow';
import { AppDataSource } from '../../src/data-source';
import { WorkflowFactory } from '../../src/workflows/WorkflowFactory';


function createTempYamlFile(content: string): string {
    const tempDir = os.tmpdir();
    const tempFilePath = path.join(tempDir, 'temp_workflow.yml');
    fs.writeFileSync(tempFilePath, content, 'utf8');

    return tempFilePath;
}
  

describe('Interdependent Tasks in Workflows', () => {
  let taskRunner: TaskRunner;
  let workflowFactory: WorkflowFactory;

  beforeAll(async () => {
    await AppDataSource.initialize();
    taskRunner = new TaskRunner(AppDataSource.getRepository(Task));
    workflowFactory = new WorkflowFactory(AppDataSource);
  });

  afterAll(async () => {
    await AppDataSource.destroy();
  });

  it('should execute dependent tasks in correct order', async () => {
    const yamlContent = `
    name: "dependent_workflow"
    steps:
      - taskType: "analysis"
        stepNumber: 1
      - taskType: "polygon_area"
        stepNumber: 2
        dependsOn: 1
      - taskType: "report_generation"
        stepNumber: 3
        dependsOn: 2
  `;

    const tempFilePath = createTempYamlFile(yamlContent);

    const workflow = await workflowFactory.createWorkflowFromYAML(tempFilePath, 'testClient', '{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}');

    const tasks = workflow.tasks.sort((a, b) => a.stepNumber - b.stepNumber);

    for (const task of tasks) {
        await taskRunner.run(task);
    }

    const updatedWorkflow = await AppDataSource.getRepository(Workflow).findOne({
        where: { workflowId: workflow.workflowId },
        relations: ['tasks']
    });

    expect(updatedWorkflow?.status).toBe('completed');
    expect(updatedWorkflow?.tasks[0].status).toBe('completed');
    expect(updatedWorkflow?.tasks[1].status).toBe('completed');
    expect(updatedWorkflow?.tasks[2].status).toBe('completed');

    expect(updatedWorkflow?.tasks[0].taskType).toBe('analysis');
    expect(updatedWorkflow?.tasks[0].stepNumber).toBe(1);
    expect(updatedWorkflow?.tasks[1].taskType).toBe('polygon_area');
    expect(updatedWorkflow?.tasks[1].stepNumber).toBe(2);
    expect(updatedWorkflow?.tasks[2].taskType).toBe('report_generation');
    expect(updatedWorkflow?.tasks[2].stepNumber).toBe(3);
  });
});
