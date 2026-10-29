package io.github.tuyucheng777.fastthreadpool.internal;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import io.github.tuyucheng777.fastthreadpool.CompletableScheduledFuture;

/// A [CompletableScheduledFuture] that runs its work on a dedicated [Thread].
///
/// Managed threads are used for workers, the scheduler, the watchdog, and the termination waiter. They
/// can be cancelled, interrupted, and joined, and they track when they last started a task so that the
/// watchdog can detect stuck tasks.
///
/// @param <T> the type of the task result
@SuppressWarnings("Comparable Implemented But Equals Not Overridden")
public abstract class ManagedThread<T> extends CompletableScheduledFuture<T> implements RunnableScheduledFuture<T> {

    private enum State {
        CREATED,
        STARTED,
        PROCESSED,
        UNPROCESSED
    }

    static final long UNSPECIFIED = 0;

    private final Thread thread;
    private final AtomicReference<State> state = new AtomicReference<>(State.CREATED);

    private long lastActivityTimeNanos = UNSPECIFIED;

    protected ManagedThread(ThreadFactory factory) {
        thread = factory.newThread(() -> {
            try {
                if (isDone()) {
                    return;
                }
                doComplete(call());
            } catch (Throwable cause) {
                doCompleteExceptionally(cause);
            } finally {
                onStop();
            }
        });
    }

    public final void start() {
        if (state.compareAndSet(State.CREATED, State.STARTED)) {
            onStart();
            thread.start();
        }
    }

    public final boolean isStarted() {
        return state.get() != State.CREATED;
    }

    public final boolean setProcessed() {
        return state.compareAndSet(State.STARTED, State.PROCESSED);
    }

    private boolean setUnprocessed() {
        return state.compareAndSet(State.STARTED, State.UNPROCESSED);
    }

    protected void onStart() {
    }

    protected void onStop() {
    }

    @Nullable
    protected abstract T call() throws Exception;

    @Override
    public final void run() {
        throw new UnsupportedOperationException();
    }

    public final String threadName() {
        return thread.getName();
    }

    public final void interrupt() {
        thread.interrupt();
    }

    public final void joinThread() {
        while (thread.isAlive()) {
            try {
                thread.join();
            } catch (InterruptedException _) {
            }
        }
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        final boolean cancelled = super.cancel(false);
        if (mayInterruptIfRunning) {
            // Even if this thread was canceled by a previous shutdown() without interruption,
            // shutdownNow() must still interrupt the task it is running.
            interrupt();
        }
        return cancelled;
    }

    final boolean cancelByShutdownNow(@Nullable List<Runnable> unprocessedTasks) {
        final boolean cancelled = cancel(true);
        if (!cancelled) {
            return false;
        }

        if (!setUnprocessed()) {
            return true;
        }

        if (unprocessedTasks != null) {
            unprocessedTasks.add(this);
        }

        return true;
    }

    final long lastActivityTimeNanos() {
        return lastActivityTimeNanos;
    }

    protected final long updateLastActivityTimeNanos() {
        final long currentTimeNanos = System.nanoTime();
        final long lastActivityTimeNanos = currentTimeNanos != 0 ? currentTimeNanos : 1;
        this.lastActivityTimeNanos = lastActivityTimeNanos;
        return lastActivityTimeNanos;
    }

    protected final void clearLastActivityTimeNanos() {
        lastActivityTimeNanos = UNSPECIFIED;
    }

    @Override
    public boolean isPeriodic() {
        return false;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return -1;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private boolean doComplete(@Nullable T value) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        return super.complete(value);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private boolean doCompleteExceptionally(Throwable cause) {
        // Clear the interrupt state of the thread so that an interrupt is not propagated to callbacks.
        Thread.interrupted();
        return super.completeExceptionally(cause);
    }

    @Override
    public final boolean complete(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void obtrudeValue(T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(Delayed o) {
        throw new UnsupportedOperationException();
    }
}
