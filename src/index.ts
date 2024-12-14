import "reflect-metadata";
import express from "express";
import analysisRoutes from "./routes/analysisRoutes";
import defaultRoute from "./routes/defaultRoute";
import { taskWorker } from "./workers/taskWorker";
import { AppDataSource } from "./data-source"; // Import the DataSource instance

const app = express();
app.use(express.json());
app.use("/analysis", analysisRoutes);
app.use("/", defaultRoute);

AppDataSource.initialize()
  .then(() => {
    // Start the worker after successful DB connection
    taskWorker();

    app.listen(3000, () => {
      console.log("Server is running at http://localhost:3000");
    });
  })
  .catch((error) => console.log(error));
