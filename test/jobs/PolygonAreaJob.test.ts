import { PolygonAreaJob } from '../../src/jobs/PolygonAreaJob';
import { Task } from '../../src/models/Task';
import { TaskStatus } from '../../src/workers/taskRunner';

describe('PolygonAreaJob', () => {
  let job: PolygonAreaJob;
  let task: Task;

  beforeEach(() => {
    job = new PolygonAreaJob();
    task = new Task();
  });

  it('should calculate the area of a valid polygon', async () => {
    task.geoJson = JSON.stringify({
      type: 'Polygon',
      coordinates: [
        [
            [-63.624885020050996, -10.311050368263523],
            [-63.624885020050996, -10.367865108370523],
            [-63.61278302732815, -10.367865108370523],
            [-63.61278302732815, -10.311050368263523],
            [-63.624885020050996, -10.311050368263523],
        ],
      ],
    });

    await job.run(task);

    expect(task.status).toBe(TaskStatus.InProgress);
    const output = JSON.parse(task.output);
    expect(output).toHaveProperty('area');
    expect(output.area).toBeGreaterThan(0);
  });

  it('should handle invalid GeoJSON gracefully', async () => {
    task.geoJson = 'invalid geojson';

    await job.run(task);

    expect(task.status).toBe(TaskStatus.Failed);
    const output = JSON.parse(task.output);
    expect(output).toHaveProperty('error', 'Invalid GeoJSON');
  });
});