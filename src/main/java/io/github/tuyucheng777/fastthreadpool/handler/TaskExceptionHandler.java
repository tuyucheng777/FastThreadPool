package io.github.tuyucheng777.fastthreadpool.handler;

import java.util.concurrent.Callable;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;

/// Handles exceptions that escape from tasks executed by a [ThreadPool].
///
/// Exceptions thrown by tasks submitted through `submit()` or the `schedule*()` methods are also
/// delivered to the returned [java.util.concurrent.CompletableFuture], so this handler is mainly useful
/// for tasks submitted through [java.util.concurrent.Executor#execute(Runnable)] and for monitoring
/// unexpected failures.
///
/// Implementations must be thread-safe. The default implementation is
/// [TaskExceptionHandler#ofDefault()], which logs failures at the `WARN` level.
public interface TaskExceptionHandler {

    /// Returns the default exception handler, which logs every failure at the `WARN` level.
    ///
    /// @return the default exception handler
    static TaskExceptionHandler ofDefault() {
        return DefaultTaskExceptionHandler.INSTANCE;
    }

    /// Handles an exception thrown by a [Runnable] task.
    ///
    /// @param task       the task that failed
    /// @param cause      the exception thrown by the task
    /// @param threadPool the pool that executed the task
    void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool);

    /// Handles an exception thrown by a [Callable] task.
    ///
    /// @param task       the task that failed
    /// @param cause      the exception thrown by the task
    /// @param threadPool the pool that executed the task
    void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool);
}
