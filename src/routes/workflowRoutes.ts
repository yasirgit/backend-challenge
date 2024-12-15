import { Router } from 'express';
import { AppDataSource } from '../data-source';
import { Workflow } from '../models/Workflow';
import { TaskStatus } from '../workers/taskRunner';
import { WorkflowStatus } from '../workflows/WorkflowFactory';

const router = Router();

router.get("/:id/status", async (req, res) => {
    const workflowId = req.params.id;
    const workflowRepository = AppDataSource.getRepository(Workflow);

    try {
        const workflow = await workflowRepository.findOne({
            where: { workflowId: workflowId },
            relations: ['tasks']
        });

        if (!workflow) {
            res.status(404).json({ message: `Workflow not found, workflowId: ${workflowId}` });

            return;
        }

        const completedTasks = workflow.tasks.filter(task => task.status === TaskStatus.Completed).length;
        const totalTasks = workflow.tasks.length;

        res.json({
            workflowId: workflow.workflowId,
            status: workflow.status,
            completedTasks,
            totalTasks
        });
    } catch (error) {
        console.error('Error fetching workflow status:', error);
        res.status(500).json({ message: 'Failed to fetch workflow status' });
    }
});


router.get('/:id/results', async (req, res) => {
    const workflowId = req.params.id;
    const workflowRepository = AppDataSource.getRepository(Workflow);

    try {
        const workflow = await workflowRepository.findOne({
            where: { workflowId }
        });

        if (!workflow) {
            res.status(404).json({ message: `Workflow not found, workflowId: ${workflowId}` });
            return;
        }

        if (workflow.status !== WorkflowStatus.Completed) {
            res.status(400).json({ message: 'Workflow is not yet completed' });
            return;
        }

        res.json({
            workflowId: workflow.workflowId,
            status: workflow.status,
            finalResult: workflow.finalResult
        });
    } catch (error: any) {
        console.error('Error fetching workflow results:', error);
        res.status(500).json({ message: 'Failed to fetch workflow results' });
    }
});

export default router;
