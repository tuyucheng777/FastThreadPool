<h1 align="left">
  <img src="docs/logo.svg" width="100" alt="Fast Thread Pool logo" align="middle" />
  &nbsp;FastThreadPool
</h1>

[![Maven Central](https://img.shields.io/maven-central/v/io.github.tuyucheng777/FastThreadPool.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.tuyucheng777/FastThreadPool)
[![Java 27](https://img.shields.io/badge/Java-27-orange.svg)](https://openjdk.org/projects/jdk/27/)
[![Line coverage](https://img.shields.io/badge/line%20coverage-91%25-brightgreen.svg)](#building-from-source)

A fast `ScheduledExecutorService` implementation backed by platform threads or virtual threads.

The platform-thread pool grows and shrinks its workers on demand, can interrupt tasks that exceed a
configured timeout, applies a pluggable task submission policy, and exports Micrometer metrics. The
virtual-thread pool starts one virtual thread per task and relies on the JDK scheduler. Every
submission and scheduling method returns a `CompletableFuture`, so tasks compose without blocking on
`Future.get()`.

## Requirements

- Java 27 or later
- Maven 3.9 or later when building from source

## Installation

Maven:

```xml
<dependency>
    <groupId>io.github.tuyucheng777</groupId>
    <artifactId>FastThreadPool</artifactId>
    <version>1.0.0</version>
</dependency>
```

Gradle:

```kotlin
implementation("io.github.tuyucheng777:FastThreadPool:1.0.0")
```

## Quick start

### Platform threads

```java
import io.github.tuyucheng777.fastthreadpool.ThreadPool;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

ThreadPool pool = ThreadPool.builder(16)
        .minNumWorkers(2)
        .idleTimeout(Duration.ofSeconds(60))
        .taskTimeout(Duration.ofSeconds(30))
        .watchdogInterval(Duration.ofSeconds(1))
        .build();

CompletableFuture<String> future = pool.submit(() -> "hello");
future.thenAccept(System.out::println);

pool.shutdown();
pool.awaitTermination();
```

`ThreadPool.of(16)` and `ThreadPool.of(2, 16)` are shortcuts for a pool with default settings.

### Virtual threads

```java
ThreadPool pool = ThreadPool.ofVirtual();

pool.submit(() -> "hello").thenAccept(System.out::println);
pool.shutdown();
pool.awaitTermination();
```

### Scheduling

```java
// One-shot task.
CompletableScheduledFuture<?> oneShot = pool.schedule(task, 5, TimeUnit.SECONDS);

// Fixed rate: the next run starts every second, even if a run takes longer than a second.
CompletableScheduledFuture<?> fixedRate =
        pool.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);

// Fixed delay: the next run starts one second after the previous one finishes.
CompletableScheduledFuture<?> fixedDelay =
        pool.scheduleWithFixedDelay(task, 0, 1, TimeUnit.SECONDS);

// Stop a periodic task.
fixedRate.cancel(false);
```

The future returned by a periodic task completes only when the task is cancelled or the pool is shut
down.

## Configuration

`ThreadPool.builder(maxNumWorkers)` returns a platform-thread builder.

| Method | Default | Description |
| --- | --- | --- |
| `minNumWorkers(int)` | `0` | Number of workers kept alive even when idle. Must be in `[0, maxNumWorkers]`. |
| `idleTimeout(Duration)` / `idleTimeout(long, TimeUnit)` | `0` (never) | Time an extra worker may stay idle before it terminates. |
| `scheduledTaskWorkers(int max)` / `scheduledTaskWorkers(int max, int min)` | disabled | Uses a dedicated worker group for scheduled tasks so long-running or periodic tasks do not starve regular submissions. |
| `taskQueue(Supplier<BlockingQueue<Runnable>>)` | `LinkedBlockingQueue` | Creates the queue used for each worker group. |
| `taskTimeout(Duration)` / `taskTimeout(long, TimeUnit)` | `0` (disabled) | Maximum time a task may run before the watchdog interrupts its worker. |
| `watchdogInterval(Duration)` / `watchdogInterval(long, TimeUnit)` | `1s` | How often the watchdog checks running tasks. Only used when `taskTimeout` is enabled. |
| `submissionHandler(TaskSubmissionHandler)` | accept all before shutdown | Admission control consulted before a task is enqueued. |
| `exceptionHandler(TaskExceptionHandler)` | log at `WARN` | Receives exceptions that escape from tasks. |
| `metrics(String prefix, Tag... tags)` | disabled | Registers Micrometer meters in the global registry. |
| `metrics(MeterRegistry registry, String prefix, Tag... tags)` | disabled | Registers Micrometer meters in a specific registry. |

`ThreadPool.builderForVirtual()` returns a virtual-thread builder. It supports `taskTimeout`,
`watchdogInterval`, and `submissionHandler`; worker-count, idle-timeout, and queue options do not apply
because each task gets its own virtual thread. Metrics and task-exception handling are not available for
the virtual-thread pool yet, so `metrics(...)` and `exceptionHandler(...)` are accepted but ignored.

## Task handlers

`TaskSubmissionHandler` decides whether a task is accepted, rejected, or dropped after logging. The
default handler accepts everything while the pool is running and rejects submissions that arrive after
`shutdown()`.

```java
TaskSubmissionHandler handler = new TaskSubmissionHandler() {
    @Override
    public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
        return isOverloaded() ? TaskAction.reject() : TaskAction.accept();
    }

    @Override
    public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
        return isOverloaded() ? TaskAction.reject() : TaskAction.accept();
    }

    @Override
    public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
        return TaskAction.log();
    }

    @Override
    public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
        return TaskAction.log();
    }
};
```

`TaskExceptionHandler` receives failures from tasks. Exceptions from `submit()` and `schedule*()` are
also delivered to the returned future, so the handler is mainly useful for monitoring and for tasks
started with `execute()`.

```java
TaskExceptionHandler handler = new TaskExceptionHandler() {
    @Override
    public void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool) {
        logger.warn("Task failed", cause);
    }

    @Override
    public void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool) {
        logger.warn("Task failed", cause);
    }
};
```

## Watchdog

When `taskTimeout` is greater than zero, a watchdog examines the active workers every
`watchdogInterval` and interrupts workers whose current task has been running longer than the timeout.
The interrupt is cooperative: tasks that ignore interruption keep running. The watchdog is terminated
after all workers have stopped, so long-running tasks are still interrupted during shutdown.

## Metrics

Metrics are enabled by passing a prefix to `metrics(...)`. The exported meters are:

| Meter | Type | Description |
| --- | --- | --- |
| `PREFIX.state` | gauge | Ordinal of the pool state: started, shutting down, or terminated. |
| `PREFIX.workers` | gauge | Number of live workers. |
| `PREFIX.busy.workers` | gauge | Number of workers currently executing a task. |
| `PREFIX.min.workers` | gauge | Configured minimum number of workers. |
| `PREFIX.max.workers` | gauge | Configured maximum number of workers. |
| `PREFIX.task.queue.size` | gauge | Number of queued tasks. |
| `PREFIX.scheduled.tasks` | gauge | Number of pending scheduled tasks. |
| `PREFIX.task.execution` | timer | Execution time of tasks; successful and failed executions carry the tag `result=success` and `result=failure`. |
| `PREFIX.task.wait` | timer | Time a task spent in the queue before execution. |

When a dedicated scheduled-task worker group is configured, the worker and queue gauges are registered
twice with the tag `scheduled=true` and `scheduled=false`; otherwise they are registered once with
`scheduled=maybe`.

The same values are available programmatically through `ThreadPool.metrics()`, which returns `null`
when metrics are disabled or unsupported.

## Shutdown

- `shutdown()` stops accepting new tasks, lets queued tasks finish, and completes
  `terminationFuture()` once all workers have stopped.
- `shutdownNow()` additionally interrupts running workers and returns the tasks that were never
  processed.
- Calling `shutdownNow()` after `shutdown()` upgrades the shutdown and interrupts the workers.
- Submissions after shutdown are handed to `TaskSubmissionHandler.handleLateSubmission`, which rejects
  them by default.
