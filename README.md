# Backend Coding Challenge

## Introduction

This repository demonstrates a backend architecture that handles asynchronous tasks, workflows, and job execution using TypeScript, Express.js, and TypeORM. The project showcases how to:

- Define and manage entities such as `Task` and `Workflow`.
- Use a `WorkflowFactory` to create workflows from YAML configurations.
- Implement a `TaskRunner` that executes jobs associated with tasks and manages task and workflow states.
- Run tasks asynchronously using a background worker.

## Key Features

1. **Entity Modeling with TypeORM**

   - **Task Entity:** Represents an individual unit of work with attributes like `taskType`, `status`, `progress`, and references to a `Workflow`.
   - **Workflow Entity:** Groups multiple tasks into a defined sequence or steps, allowing complex multi-step processes.

2. **Workflow Creation from YAML**

   - Use `WorkflowFactory` to load workflow definitions from a YAML file.
   - Dynamically create workflows and tasks without code changes by updating YAML files.

3. **Asynchronous Task Execution**

   - A background worker (`taskWorker`) continuously polls for `queued` tasks.
   - The `TaskRunner` runs the appropriate job based on a task’s `taskType`.

4. **Robust Status Management**

   - `TaskRunner` updates the status of tasks (from `queued` to `in_progress`, `completed`, or `failed`).
   - Workflow status is evaluated after each task completes, ensuring you know when the entire workflow is `completed` or `failed`.

5. **Dependency Injection and Decoupling**
   - `TaskRunner` takes in only the `Task` and determines the correct job internally.
   - `TaskRunner` handles task state transitions, leaving the background worker clean and focused on orchestration.

## Project Structure

```
src
├─ models/
│   ├─ world_data.json  # Contains world data for analysis
│
├─ models/
│   ├─ Result.ts        # Defines the Result entity
│   ├─ Task.ts          # Defines the Task entity
│   ├─ Workflow.ts      # Defines the Workflow entity
│
├─ jobs/
│   ├─ Job.ts           # Job interface
│   ├─ JobFactory.ts    # getJobForTaskType function for mapping taskType to a Job
│   ├─ TaskRunner.ts    # Handles job execution & task/workflow state transitions
│   ├─ DataAnalysisJob.ts (example)
│   ├─ EmailNotificationJob.ts (example)
│   ├─ PolygonAreaJob.ts (Calculate Polygon Area)
│   ├─ ReportGenerationJob.ts (Generate a Report)
│
├─ workflows/
│   ├─ WorkflowFactory.ts  # Creates workflows & tasks from a YAML definition
│
├─ workers/
│   ├─ taskWorker.ts    # Background worker that fetches queued tasks & runs them
│   ├─ taskRunner.ts    # Run tasks and store information in database
│
├─ routes/
│   ├─ analysisRoutes.ts # POST /analysis endpoint to create workflows
│   ├─ workflowRoutes.ts # GET /workflows endpoint to return workflow details
│
├─ data-source.ts       # TypeORM DataSource configuration
└─ index.ts             # Express.js server initialization & starting the worker

test
├─ jobs/
│   ├─ PolygonAreaJob.test.ts
│   ├─ ReportGenerationJob.test.ts
├─ workers/
│   ├─ taskRunner.test.ts
```

## Enhancements

1. Added a New Job to Calculate Polygon Area
2. Added a Job to Generate a Report
3. Support Interdependent Tasks in Workflows
4. Save Final Workflow Results
5. New Endpoint for Getting Workflow Status
    - [GET] /workflows/:workflowId/status
6. New Endpoint for Retrieving Workflow Results
    - [GET] /workflows/:workflowId/results
7. Unit tests to test the flow

## Getting Started

### Prerequisites

- Node.js (LTS recommended)
- npm or yarn
- SQLite or another supported database

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yasirgit/backend-challenge.git
   cd backend-challenge
   ```

2. **Install dependencies:**

   ```bash
   npm install
   ```

### Running the Application

1. **Compile TypeScript (optional if using `ts-node`):**

   ```bash
   npx tsc
   ```

2. **Start the server:**

   ```bash
   npm start
   ```

3. **Run tests:**

   ```bash
   npm test
   ```


### Testing the Features

1. **Download postman to test the APIs:**

   [Download Postman](https://www.postman.com/downloads/)

2. **Import postman collection and environment:**

   1. [Backend Challenges.postman_collection.json](https://github.com/yasirgit/backend-challenge/blob/main/docs/Backend%20Challenges.postman_collection.json)
   2. [Backend_Challenges_ENV.postman_environment.json](https://github.com/yasirgit/backend-challenge/blob/main/docs/Backend_Challenges_ENV.postman_environment.json)

3. **Endpoints:**

   ```bash
   1. [POST] /analysis (Trigger this one at first to create the Workflow and its corresponding Tasks and results)
   2. [GET] /workflows/{{workflowId}}/status (retrieve the current status of a workflow)
   2. [GET] /workflows/{{workflowId}}/results (retrieve the final results of a completed workflow)
   ```

4. **Requests and responses of the APIs:**

   [API Doc](https://app.swaggerhub.com/apis/yasirgit/backend-challenges/1.0.0)

5. **Interact with the database:**

   [DB Browser for SQLite](https://sqlitebrowser.org/dl/)

5. **Run the app in debug mode for development purpose (only for vscode):**

   ```bash
   1. Stop running app (ctr + c)
   2. Click Run and Debug button, and then Run Script
   ```