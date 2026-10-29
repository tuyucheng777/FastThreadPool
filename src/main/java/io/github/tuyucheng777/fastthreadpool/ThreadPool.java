package io.github.tuyucheng777.fastthreadpool;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;

/// A [ScheduledExecutorService] whose tasks run on either platform threads or virtual threads.
///
/// Unlike the executors returned by [java.util.concurrent.Executors], this interface exposes
/// [CompletableFuture]s for every submission and scheduling method, so that tasks can be composed and
/// completed asynchronously without blocking on [java.util.concurrent.Future#get].
///
/// The platform-thread implementation grows and shrinks its worker threads on demand, can interrupt tasks
/// that do not finish within a configured timeout, and exports [Micrometer](https://micrometer.io) metrics.
/// The virtual-thread implementation starts one virtual thread per task and relies on the JDK scheduler.
///
/// ## Example
///
/// ```java
/// ThreadPool pool = ThreadPool.builder(8)
///         .minNumWorkers(2)
///         .taskTimeout(Duration.ofSeconds(30))
///         .metrics("my.pool")
///         .build();
///
/// pool.submit(() -> 42).thenAccept(System.out::println);
/// pool.shutdown();
/// pool.awaitTermination();
/// ```
///
/// Implementations are thread-safe. A task submitted after [ThreadPool#shutdown] is handled by
/// [TaskSubmissionHandler#handleLateSubmission(Runnable, ThreadPool)], which rejects it by default.
///
/// @see ThreadPoolBuilder
/// @see VirtualThreadPoolBuilder
public interface ThreadPool extends ScheduledExecutorService {

    /// Creates a platform-thread pool whose worker count can grow up to `maxNumWorkers`.
    ///
    /// @param maxNumWorkers the maximum number of worker threads, which must be greater than `0`
    /// @return a new pool that is not shut down
    static ThreadPool of(int maxNumWorkers) {
        return builder(maxNumWorkers).build();
    }

    /// Creates a platform-thread pool with at least `minNumWorkers` and at most `maxNumWorkers` workers.
    ///
    /// @param minNumWorkers the number of worker threads kept alive, which must be in `[0, maxNumWorkers]`
    /// @param maxNumWorkers the maximum number of worker threads, which must be greater than `0`
    /// @return a new pool that is not shut down
    static ThreadPool of(int minNumWorkers, int maxNumWorkers) {
        return builder(maxNumWorkers).minNumWorkers(minNumWorkers).build();
    }

    /// Returns a builder for a platform-thread pool.
    ///
    /// @param maxNumWorkers the maximum number of worker threads, which must be greater than `0`
    /// @return a new builder
    static ThreadPoolBuilder builder(int maxNumWorkers) {
        return new ThreadPoolBuilder(maxNumWorkers);
    }

    /// Creates a virtual-thread pool that starts a virtual thread per task.
    ///
    /// @return a new pool that is not shut down
    static ThreadPool ofVirtual() {
        return builderForVirtual().build();
    }

    /// Returns a builder for a virtual-thread pool.
    ///
    /// @return a new builder
    static VirtualThreadPoolBuilder builderForVirtual() {
        return new VirtualThreadPoolBuilder();
    }

    /// Returns whether this pool executes tasks on virtual threads.
    ///
    /// @return `true` for a virtual-thread pool, `false` for a platform-thread pool
    boolean isVirtual();

    /// Submits a value-returning task for execution.
    ///
    /// @param task the task to submit
    /// @param <T>  the type of the task result
    /// @return a future that completes with the task result, or exceptionally if the task fails
    @Override
    <T> CompletableFuture<T> submit(Callable<T> task);

    /// Submits a task for execution and returns the given result when the task completes.
    ///
    /// @param task   the task to submit
    /// @param result the result to return when the task completes
    /// @param <T>    the type of the result
    /// @return a future that completes with `result` once the task has finished
    @Override
    <T> CompletableFuture<T> submit(Runnable task, T result);

    /// Submits a [Runnable] task for execution.
    ///
    /// @param task the task to submit
    /// @return a future that completes with `null` once the task has finished
    @Override
    CompletableFuture<?> submit(Runnable task);

    /// Schedules a one-shot [Runnable] task.
    ///
    /// @param command the task to execute
    /// @param delay   the delay before execution, which must not be negative
    /// @param unit    the time unit of `delay`
    /// @return a future that completes once the task has finished, or exceptionally if the task fails
    @Override
    CompletableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    /// Schedules a one-shot value-returning task.
    ///
    /// @param callable the task to execute
    /// @param delay    the delay before execution, which must not be negative
    /// @param unit     the time unit of `delay`
    /// @param <V>      the type of the task result
    /// @return a future that completes with the task result, or exceptionally if the task fails
    @Override
    <V> CompletableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

    /// Schedules a task that runs periodically with a fixed rate.
    ///
    /// If an execution takes longer than `period`, the next execution starts immediately after the
    /// previous one finishes. The returned future completes only when the task is cancelled or the pool
    /// is shut down.
    ///
    /// @param command      the task to execute
    /// @param initialDelay the delay before the first execution
    /// @param period       the interval between the starts of consecutive executions, which must be greater than `0`
    /// @param unit         the time unit of `initialDelay` and `period`
    /// @return a future that completes when the periodic task stops
    @Override
    CompletableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    /// Schedules a task that runs periodically with a fixed delay between executions.
    ///
    /// The next execution starts only after the previous one has finished and `delay` has elapsed. The
    /// returned future completes only when the task is cancelled or the pool is shut down.
    ///
    /// @param command      the task to execute
    /// @param initialDelay the delay before the first execution
    /// @param delay        the delay between the end of an execution and the start of the next one,
    ///                     which must be greater than `0`
    /// @param unit         the time unit of `initialDelay` and `delay`
    /// @return a future that completes when the periodic task stops
    @Override
    CompletableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);

    /// Returns the metrics exported by this pool.
    ///
    /// @return the metrics, or `null` if metrics were not enabled or this pool does not support them
    @Nullable
    ThreadPoolMetrics metrics();

    /// Waits until this pool has terminated.
    ///
    /// This is equivalent to `terminationFuture().join()`, but it declares [InterruptedException] so that
    /// callers can handle interruption explicitly.
    ///
    /// @throws InterruptedException if the current thread is interrupted while waiting
    void awaitTermination() throws InterruptedException;

    /// Returns a future that completes when this pool has terminated.
    ///
    /// @return the termination future
    CompletableFuture<?> terminationFuture();
}
