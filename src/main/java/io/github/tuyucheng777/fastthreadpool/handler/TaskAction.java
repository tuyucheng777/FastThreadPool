package io.github.tuyucheng777.fastthreadpool.handler;

import java.util.concurrent.Callable;

/// The decision returned by a [TaskSubmissionHandler] for a submitted task.
///
/// A task action describes what the pool should do with a task that a submission handler has already
/// inspected. The built-in actions are:
///
/// - [#accept()] runs the task in the pool.
/// - [#reject()] throws a [java.util.concurrent.RejectedExecutionException].
/// - [#log()] drops the task after logging a warning.
///
/// Implementations may also perform arbitrary work, as long as [#doAction(Runnable)] and
/// [#doAction(Callable)] report the outcome of the rejected task to the submitter.
public interface TaskAction {

    /// Returns the action that accepts a task and lets the pool execute it.
    ///
    /// @return the accept action
    static TaskAction accept() {
        return TaskActions.ACCEPT;
    }

    /// Returns the action that rejects a task by throwing a
    /// [java.util.concurrent.RejectedExecutionException].
    ///
    /// @return the reject action
    static TaskAction reject() {
        return TaskActions.REJECT;
    }

    /// Returns the action that drops a rejected task after logging a warning
    /// at the `WARN` level.
    ///
    /// @return the log action
    static TaskAction log() {
        return TaskActions.LOG;
    }

    /// Applies this action to a [Runnable] task.
    ///
    /// @param task the rejected task
    void doAction(Runnable task);

    /// Applies this action to a [Callable] task.
    ///
    /// @param task the rejected task
    void doAction(Callable<?> task);
}
