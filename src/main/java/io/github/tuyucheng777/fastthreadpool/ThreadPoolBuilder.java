package io.github.tuyucheng777.fastthreadpool;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import io.github.tuyucheng777.fastthreadpool.internal.AbstractThreadPoolBuilder;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.platform.PlatformThreadPool;
import io.github.tuyucheng777.fastthreadpool.platform.PlatformWorkerGroup;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// Builds a platform-thread [ThreadPool].
///
/// A builder is obtained from [ThreadPool#builder(int)]; every configuration method returns the builder
/// itself, so calls can be chained:
///
/// ```java
/// ThreadPool pool = ThreadPool.builder(16)
///         .minNumWorkers(4)
///         .idleTimeout(Duration.ofSeconds(60))
///         .taskTimeout(Duration.ofSeconds(30))
///         .scheduledTaskWorkers(2)
///         .metrics("my.pool")
///         .build();
/// ```
///
/// A builder is not thread-safe and should be used from a single thread.
///
/// @see ThreadPool
public final class ThreadPoolBuilder extends AbstractThreadPoolBuilder<ThreadPoolBuilder> {

    private final int maxNumWorkers;
    private int minNumWorkers;
    private long idleTimeoutNanos;
    private boolean scheduledTaskWorkersEnabled;
    private int maxNumScheduledTaskWorkers;
    private int minNumScheduledTaskWorkers;
    private Supplier<BlockingQueue<Runnable>> taskQueueFactory = LinkedBlockingQueue::new;

    ThreadPoolBuilder(int maxNumWorkers) {
        this.maxNumWorkers = checkMaxNumWorkers("maxNumWorkers", maxNumWorkers);
    }

    /// Sets the number of worker threads that are kept alive even when they are idle.
    ///
    /// The default is `0`.
    ///
    /// @param minNumWorkers the minimum number of workers, which must be in `[0, maxNumWorkers]`
    /// @return this builder
    public ThreadPoolBuilder minNumWorkers(int minNumWorkers) {
        this.minNumWorkers = checkMinNumWorkers("minNumWorkers", minNumWorkers, "maxNumWorkers", maxNumWorkers);
        return this;
    }

    /// Sets how long an idle worker may live before it is terminated.
    ///
    /// A zero or negative value disables idle expiration, so workers above the minimum are kept alive
    /// until the pool is shut down.
    ///
    /// @param idleTimeout the idle timeout
    /// @param unit        the time unit of `idleTimeout`
    /// @return this builder
    public ThreadPoolBuilder idleTimeout(long idleTimeout, TimeUnit unit) {
        checkArgument(idleTimeout >= 0, "idleTimeout: %s (expected: >= 0)", idleTimeout);
        idleTimeoutNanos = requireNonNull(unit, "unit").toNanos(idleTimeout);
        return this;
    }

    /// Sets how long an idle worker may live before it is terminated.
    ///
    /// @param idleTimeout the idle timeout, which must not be negative
    /// @return this builder
    public ThreadPoolBuilder idleTimeout(Duration idleTimeout) {
        requireNonNull(idleTimeout, "idleTimeout");
        checkArgument(!idleTimeout.isNegative(), "idleTimeout: %s (expected: >= 0)", idleTimeout);
        return idleTimeout(idleTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Sets the factory that creates the queue used to hold submitted tasks.
    ///
    /// The default is [LinkedBlockingQueue], which is unbounded. Supplying a bounded queue is a simple way
    /// to apply backpressure: when the queue is full, the submission handler decides what to do with the
    /// task.
    ///
    /// @param taskQueueFactory the factory that creates a new queue for the regular workers
    /// @return this builder
    public ThreadPoolBuilder taskQueue(Supplier<? extends BlockingQueue<Runnable>> taskQueueFactory) {
        @SuppressWarnings("unchecked") final Supplier<BlockingQueue<Runnable>> cast =
                (Supplier<BlockingQueue<Runnable>>) requireNonNull(taskQueueFactory, "taskQueueFactory");
        this.taskQueueFactory = cast;
        return this;
    }

    /// Enables a dedicated group of workers for scheduled tasks.
    ///
    /// By default, scheduled tasks run on the same workers as regular tasks. Dedicating workers prevents
    /// long-running or periodic tasks from starving regular submissions.
    ///
    /// @param maxNumScheduledTaskWorkers the maximum number of scheduled-task workers, which must be greater than `0`
    /// @return this builder
    public ThreadPoolBuilder scheduledTaskWorkers(int maxNumScheduledTaskWorkers) {
        return scheduledTaskWorkers(maxNumScheduledTaskWorkers, 0);
    }

    /// Enables a dedicated group of workers for scheduled tasks.
    ///
    /// @param maxNumScheduledTaskWorkers the maximum number of scheduled-task workers, which must be greater than `0`
    /// @param minNumScheduledTaskWorkers the minimum number of scheduled-task workers, which must be in
    ///                                    `[0, maxNumScheduledTaskWorkers]`
    /// @return this builder
    public ThreadPoolBuilder scheduledTaskWorkers(int maxNumScheduledTaskWorkers,
                                                  int minNumScheduledTaskWorkers) {
        checkMaxNumWorkers("maxNumScheduledTaskWorkers", maxNumScheduledTaskWorkers);
        checkMinNumWorkers("minNumScheduledTaskWorkers", minNumScheduledTaskWorkers,
                "maxNumScheduledTaskWorkers", maxNumScheduledTaskWorkers);

        scheduledTaskWorkersEnabled = true;
        this.maxNumScheduledTaskWorkers = maxNumScheduledTaskWorkers;
        this.minNumScheduledTaskWorkers = minNumScheduledTaskWorkers;
        return this;
    }

    private static int checkMaxNumWorkers(String paramName, int maxNumWorkers) {
        checkArgument(maxNumWorkers > 0, "%s: %s (expected: > 0)", paramName, maxNumWorkers);
        return maxNumWorkers;
    }

    private static int checkMinNumWorkers(String minNumWorkersName, int minNumWorkers,
                                          String maxNumWorkersName, int maxNumWorkers) {
        checkArgument(minNumWorkers >= 0 && minNumWorkers <= maxNumWorkers,
                "%s: %s (expected: [0, %s (%s)]",
                minNumWorkersName, minNumWorkers,
                maxNumWorkersName, maxNumWorkers);
        return minNumWorkers;
    }

    @Override
    protected ThreadPool build(TaskSubmissionHandler submissionHandler, TaskExceptionHandler exceptionHandler,
                     long taskTimeoutNanos, long watchdogIntervalNanos, MeterRegistry meterRegistry,
                     @Nullable String metricPrefix, List<Tag> metricTags) {

        final BlockingQueue<Runnable> taskQueue = taskQueueFactory.get();
        final PlatformWorkerGroup workers = new PlatformWorkerGroup(
                minNumWorkers, maxNumWorkers,
                idleTimeoutNanos, taskQueue,
                submissionHandler, exceptionHandler);

        final PlatformWorkerGroup scheduledWorkers;
        if (scheduledTaskWorkersEnabled) {
            final BlockingQueue<Runnable> scheduledTaskQueue = taskQueueFactory.get();
            scheduledWorkers = new PlatformWorkerGroup(
                    minNumScheduledTaskWorkers,
                    maxNumScheduledTaskWorkers,
                    idleTimeoutNanos, scheduledTaskQueue,
                    submissionHandler, exceptionHandler);
        } else {
            scheduledWorkers = null;
        }

        final PlatformThreadPool pool;
        if (scheduledTaskWorkersEnabled) {
            pool = new PlatformThreadPool(workers, scheduledWorkers,
                    taskTimeoutNanos, watchdogIntervalNanos,
                    meterRegistry, metricPrefix, metricTags);
        } else {
            pool = new PlatformThreadPool(workers, workers,
                    taskTimeoutNanos, watchdogIntervalNanos,
                    meterRegistry, metricPrefix, metricTags);
        }

        workers.setParent(pool);
        if (scheduledWorkers != null) {
            scheduledWorkers.setParent(pool);
        }

        return pool;
    }
}
