# FluxOrchestrator

A lightweight workflow orchestration engine and real-time execution visualizer. 

It allows users to define multi-step directed acyclic graph (DAG) pipelines, execute them asynchronously through a background queue, and watch step transitions and terminal logs live via WebSockets.

---

## What It Does

In production systems, automated jobs (ETL pipelines, build & deployment flows, scheduled syncs) often consist of tasks with strict dependencies: Step B cannot start until Step A finishes, but Step C and Step D can run in parallel once Step B completes.

FluxOrchestrator provides a full-stack implementation of this pattern:
1. **Define Pipelines:** Create workflows where steps declare explicit parent dependencies (`DependsOn`).
2. **Non-Blocking Queue:** Triggering a pipeline pushes the job to an in-memory channel and returns immediately with a `202 Accepted` and a `RunId`.
3. **Background Worker Engine:** A background service evaluates the DAG, picks tasks whose dependencies have succeeded, executes them concurrently where possible, and handles state transitions (`Queued` → `Running` → `Succeeded` / `Failed`).
4. **Live Visualization:** A browser dashboard updates step statuses (pending, running, completed) and streams execution logs in real time over WebSockets without page polling.

---

## Why This Tech Stack?

Instead of building a standard CRUD application, this project was designed to solve an asynchronous coordination problem across modern frontend and backend tools:

- **.NET 8 (ASP.NET Core Minimal APIs):** Fast, lightweight endpoints without MVC controller overhead.
- **`System.Threading.Channels` + `BackgroundService`:** Uses built-in concurrent queues to decouple HTTP request lifecycles from long-running task execution.
- **SignalR:** Pushes real-time execution state and log chunks directly to connected clients over WebSockets.
- **Angular 18+ (Shell & State):** Serves as the primary platform application. Built with Standalone components, modern `@if` / `@for` template control flow, and Angular Signals for reactive client state.
- **React (Interactive DAG Canvas):** Renders the node-and-edge workflow graph using React Flow. Embedded inside the Angular shell to leverage React's ecosystem for visual canvas diagramming.

---

## Key Features

- **DAG Dependency Resolution:** Respects task dependencies; automatically triggers parallel branches when shared prerequisites finish.
- **Real-Time Push Updates:** WebSockets push status changes to the client as they happen.
- **Streaming Terminal Logs:** Shows live console output for each running task.
- **In-Memory & Lightweight:** Runs out of the box using EF Core In-Memory storage without requiring external database or Redis instances for local development.
