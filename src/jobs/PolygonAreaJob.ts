import { Job } from './Job';
import {Task} from "../models/Task";
import * as turf from '@turf/turf';
import {TaskStatus} from "../workers/taskRunner";


export class PolygonAreaJob implements Job {
    async run(task: Task): Promise<void> {
        try {
            const geoJson = JSON.parse(task.geoJson);
            const area = turf.area(geoJson);
            task.output = JSON.stringify({ area });
            task.status = TaskStatus.InProgress;
        } catch (error) {
            task.status = TaskStatus.Failed;
            task.output = JSON.stringify({ error: 'Invalid GeoJSON' });
        }
        return;
    }
}