package io.github.tuyucheng777.fastthreadpool.handler;

import java.util.concurrent.Callable;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;

/// The default [TaskSubmissionHandler].
///
/// It accepts every task while the pool is running and rejects tasks submitted after shutdown by
/// throwing a [java.util.concurrent.RejectedExecutionException].
enum DefaultTaskSubmissionHandler implements TaskSubmissionHandler {

    INSTANCE;

    @Override
    public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
        return TaskAction.accept();
    }

    @Override
    public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
        return TaskAction.reject();
    }

    @Override
    public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
        return TaskAction.reject();
    }
}
