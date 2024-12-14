import { Entity, PrimaryGeneratedColumn, Column, OneToMany } from 'typeorm';
import { Task } from './Task';
import {WorkflowStatus} from "../workflows/WorkflowFactory";

@Entity({ name: 'workflows' })
export class Workflow {
    @PrimaryGeneratedColumn('uuid')
    workflowId!: string;

    @Column()
    clientId!: string;

    @Column({ default: WorkflowStatus.Initial })
    status!: WorkflowStatus;

    @OneToMany(() => Task, task => task.workflow)
    tasks!: Task[];
}