package io.github.tuyucheng777.fastthreadpool.virtual;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/// A [ManagedVirtualThread] that completes when its one-shot task finishes.
///
/// @param <V> the type of the task result
final class VirtualThreadFutureTask<V> extends ManagedVirtualThread<V> {

    VirtualThreadFutureTask(VirtualThreadPool parent, Runnable task, @Nullable V result) {
        this(parent, Executors.callable(task, result));
    }

    VirtualThreadFutureTask(VirtualThreadPool parent, Callable<V> task) {
        super(parent, task);
    }

    @Nullable
    @Override
    protected V call() throws Exception {
        if (setProcessed()) {
            return super.call();
        } else {
            return null;
        }
    }
}
