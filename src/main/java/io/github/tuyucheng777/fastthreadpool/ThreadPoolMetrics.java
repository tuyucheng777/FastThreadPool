package io.github.tuyucheng777.fastthreadpool;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import javax.annotation.Nullable;

/// The Micrometer meters exported by a [ThreadPool] when metrics are enabled.
///
/// Each meter is registered with the prefix and tags configured through
/// `ThreadPool.builder(...).metrics(...)`, and gauges that only exist for platform-thread pools are
/// `null` for virtual-thread pools.
///
/// | Accessor | Meter | Description |
/// | --- | --- | --- |
/// | [#state()] | `PREFIX.state` | Ordinal of the pool state: started, shutting down, or terminated. |
/// | [#workers()] | `PREFIX.workers` | Number of live worker threads. |
/// | [#busyWorkers()] | `PREFIX.busy.workers` | Number of workers currently executing a task. |
/// | [#minWorkers()] | `PREFIX.min.workers` | Configured minimum number of workers. |
/// | [#maxWorkers()] | `PREFIX.max.workers` | Configured maximum number of workers. |
/// | [#taskQueueSize()] | `PREFIX.task.queue.size` | Number of queued tasks. |
/// | [#scheduledTasks()] | `PREFIX.scheduled.tasks` | Number of pending scheduled tasks. |
/// | [#taskWaitTimer()] | `PREFIX.task.wait` | Time a task spent in the queue before execution. |
/// | [#successfulTaskExecutionTimer()] | `PREFIX.task.execution` | Execution time of successful tasks. |
/// | [#failedTaskExecutionTimer()] | `PREFIX.task.execution` | Execution time of failed tasks. |
///
/// @see ThreadPool#metrics()
public final class ThreadPoolMetrics {

    private final Gauge state;
    private final Gauge workers;
    @Nullable
    private final Gauge scheduledTaskWorkers;
    private final Gauge busyWorkers;
    @Nullable
    private final Gauge busyScheduledTaskWorkers;
    private final Gauge minWorkers;
    @Nullable
    private final Gauge minScheduledTaskWorkers;
    private final Gauge maxWorkers;
    @Nullable
    private final Gauge maxScheduledTaskWorkers;
    private final Gauge taskQueueSize;
    @Nullable
    private final Gauge scheduledTaskQueueSize;
    private final Timer successfulTaskExecutionTimer;
    private final Timer failedTaskExecutionTimer;
    private final Timer taskWaitTimer;
    private final Gauge scheduledTasks;

    /// Creates a metrics holder from the meters registered by a platform-thread pool.
    ///
    /// This constructor is public only so that the pool implementation and its tests can create a
    /// holder; applications should not need to call it.
    ///
    /// @param state                        the gauge that reports the pool state
    /// @param workers                      the gauge that reports the number of workers
    /// @param scheduledTaskWorkers         the gauge for dedicated scheduled-task workers, or `null`
    /// @param busyWorkers                  the gauge that reports the number of busy workers
    /// @param busyScheduledTaskWorkers     the gauge for busy scheduled-task workers, or `null`
    /// @param minWorkers                   the gauge that reports the configured minimum number of workers
    /// @param minScheduledTaskWorkers      the gauge for the minimum number of scheduled-task workers, or `null`
    /// @param maxWorkers                   the gauge that reports the configured maximum number of workers
    /// @param maxScheduledTaskWorkers      the gauge for the maximum number of scheduled-task workers, or `null`
    /// @param taskQueueSize                the gauge that reports the queue size
    /// @param scheduledTaskQueueSize       the gauge for the scheduled-task queue size, or `null`
    /// @param successfulTaskExecutionTimer the timer of successful task executions
    /// @param failedTaskExecutionTimer     the timer of failed task executions
    /// @param taskWaitTimer                the timer of the time tasks spent in the queue
    /// @param scheduledTasks               the gauge that reports the number of pending scheduled tasks
    public ThreadPoolMetrics(Gauge state,
                             Gauge workers, @Nullable Gauge scheduledTaskWorkers,
                             Gauge busyWorkers, @Nullable Gauge busyScheduledTaskWorkers,
                             Gauge minWorkers, @Nullable Gauge minScheduledTaskWorkers,
                             Gauge maxWorkers, @Nullable Gauge maxScheduledTaskWorkers,
                             Gauge taskQueueSize, @Nullable Gauge scheduledTaskQueueSize,
                             Timer successfulTaskExecutionTimer,
                             Timer failedTaskExecutionTimer,
                             Timer taskWaitTimer,
                             Gauge scheduledTasks) {
        this.state = state;
        this.workers = workers;
        this.scheduledTaskWorkers = scheduledTaskWorkers;
        this.busyWorkers = busyWorkers;
        this.busyScheduledTaskWorkers = busyScheduledTaskWorkers;
        this.minWorkers = minWorkers;
        this.minScheduledTaskWorkers = minScheduledTaskWorkers;
        this.maxWorkers = maxWorkers;
        this.maxScheduledTaskWorkers = maxScheduledTaskWorkers;
        this.taskQueueSize = taskQueueSize;
        this.scheduledTaskQueueSize = scheduledTaskQueueSize;
        this.successfulTaskExecutionTimer = successfulTaskExecutionTimer;
        this.failedTaskExecutionTimer = failedTaskExecutionTimer;
        this.taskWaitTimer = taskWaitTimer;
        this.scheduledTasks = scheduledTasks;
    }

    /// Returns the gauge that reports the ordinal of the pool state.
    ///
    /// @return the state gauge
    public Gauge state() {
        return state;
    }

    /// Returns the gauge that reports the total number of worker threads.
    ///
    /// @return the worker-count gauge
    public Gauge workers() {
        return workers;
    }

    /// Returns the gauge that reports the number of dedicated scheduled-task workers.
    ///
    /// @return the scheduled-task worker gauge, or `null` if scheduled tasks share the regular workers
    @Nullable
    public Gauge scheduledTaskWorkers() {
        return scheduledTaskWorkers;
    }

    /// Returns the gauge that reports the number of workers currently executing a task.
    ///
    /// @return the busy-worker gauge
    public Gauge busyWorkers() {
        return busyWorkers;
    }

    /// Returns the gauge that reports the number of busy dedicated scheduled-task workers.
    ///
    /// @return the busy scheduled-task worker gauge, or `null` if scheduled tasks share the regular workers
    @Nullable
    public Gauge busyScheduledTaskWorkers() {
        return busyScheduledTaskWorkers;
    }

    /// Returns the gauge that reports the configured minimum number of workers.
    ///
    /// @return the minimum-worker gauge
    public Gauge minWorkers() {
        return minWorkers;
    }

    /// Returns the gauge that reports the configured minimum number of dedicated scheduled-task workers.
    ///
    /// @return the minimum scheduled-task worker gauge, or `null` if scheduled tasks share the regular workers
    @Nullable
    public Gauge minScheduledTaskWorkers() {
        return minScheduledTaskWorkers;
    }

    /// Returns the gauge that reports the configured maximum number of workers.
    ///
    /// @return the maximum-worker gauge
    public Gauge maxWorkers() {
        return maxWorkers;
    }

    /// Returns the gauge that reports the configured maximum number of dedicated scheduled-task workers.
    ///
    /// @return the maximum scheduled-task worker gauge, or `null` if scheduled tasks share the regular workers
    @Nullable
    public Gauge maxScheduledTaskWorkers() {
        return maxScheduledTaskWorkers;
    }

    /// Returns the gauge that reports the number of queued tasks.
    ///
    /// @return the task-queue size gauge
    public Gauge taskQueueSize() {
        return taskQueueSize;
    }

    /// Returns the gauge that reports the number of queued scheduled tasks.
    ///
    /// @return the scheduled-task queue size gauge, or `null` if scheduled tasks share the regular workers
    @Nullable
    public Gauge scheduledTaskQueueSize() {
        return scheduledTaskQueueSize;
    }

    /// Returns the timer that records the execution time of successful tasks.
    ///
    /// @return the successful-task execution timer
    public Timer successfulTaskExecutionTimer() {
        return successfulTaskExecutionTimer;
    }

    /// Returns the timer that records the execution time of failed tasks.
    ///
    /// @return the failed-task execution timer
    public Timer failedTaskExecutionTimer() {
        return failedTaskExecutionTimer;
    }

    /// Returns the timer that records how long tasks waited in the queue before execution.
    ///
    /// @return the task-wait timer
    public Timer taskWaitTimer() {
        return taskWaitTimer;
    }

    /// Returns the gauge that reports the number of pending scheduled tasks.
    ///
    /// @return the pending scheduled-task gauge
    public Gauge scheduledTasks() {
        return scheduledTasks;
    }
}
