package io.github.tuyucheng777.fastthreadpool.virtual;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import io.github.tuyucheng777.fastthreadpool.internal.ManagedThread;

/// A [ManagedThread] that runs its task on a virtual thread and reports its lifecycle to a [VirtualThreadPool].
///
/// @param <V> the type of the task result
abstract class ManagedVirtualThread<V> extends ManagedThread<V> {

    private final VirtualThreadPool parent;
    private final Callable<V> task;

    ManagedVirtualThread(VirtualThreadPool parent, Callable<V> task) {
        super(VirtualThreadPool.threadFactory);
        this.parent = parent;
        this.task = task;
    }

    @Override
    protected final void onStart() {
        parent.addTask(this);
    }

    @Override
    protected final void onStop() {
        parent.removeTask(this);
    }

    @Nullable
    @Override
    protected V call() throws Exception {
        activate();
        try {
            return task.call();
        } finally {
            deactivate();
        }
    }

    private void activate() {
        updateLastActivityTimeNanos();
        parent.addActiveTask(this);
    }

    private void deactivate() {
        parent.removeActiveTask(this);
        clearLastActivityTimeNanos();
    }
}
