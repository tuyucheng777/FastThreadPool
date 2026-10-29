package io.github.tuyucheng777.fastthreadpool.handler;

import java.util.concurrent.Callable;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;

/// Decides whether a submitted task should be accepted, rejected, or logged.
///
/// The pool consults a submission handler before it enqueues a task, which allows application code to
/// implement admission control (for example, rejecting tasks when a dependency is unavailable) without
/// wrapping the executor.
///
/// A submission handler must be thread-safe. The default implementation is
/// [TaskSubmissionHandler#ofDefault()], which accepts every task while the pool is running and rejects
/// every task submitted after shutdown.
///
/// @see TaskAction
public interface TaskSubmissionHandler {

    /// Returns the default submission handler.
    ///
    /// The default handler accepts all tasks submitted before shutdown and rejects all tasks submitted
    /// afterwards by throwing a [java.util.concurrent.RejectedExecutionException].
    ///
    /// @return the default submission handler
    static TaskSubmissionHandler ofDefault() {
        return DefaultTaskSubmissionHandler.INSTANCE;
    }

    /// Decides what to do with a task submitted before the pool is shut down.
    ///
    /// @param task       the task being submitted
    /// @param threadPool the pool that received the task
    /// @return the action to apply; [TaskAction#accept()] enqueues the task
    TaskAction handleSubmission(Runnable task, ThreadPool threadPool);

    /// Decides what to do with a value-returning task submitted before the pool is shut down.
    ///
    /// @param task       the task being submitted
    /// @param threadPool the pool that received the task
    /// @return the action to apply; [TaskAction#accept()] enqueues the task
    TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool);

    /// Decides what to do with a task submitted after the pool has been shut down.
    ///
    /// A handler must never return [TaskAction#accept()] from this method, because the pool can no longer
    /// execute the task.
    ///
    /// @param task       the task being submitted
    /// @param threadPool the pool that received the task
    /// @return the action to apply
    TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool);

    /// Decides what to do with a value-returning task submitted after the pool has been shut down.
    ///
    /// A handler must never return [TaskAction#accept()] from this method, because the pool can no longer
    /// execute the task.
    ///
    /// @param task       the task being submitted
    /// @param threadPool the pool that received the task
    /// @return the action to apply
    TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool);
}
