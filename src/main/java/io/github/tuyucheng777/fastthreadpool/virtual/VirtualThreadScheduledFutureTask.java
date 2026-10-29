package io.github.tuyucheng777.fastthreadpool.virtual;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolUtil;

/// A [ManagedVirtualThread] that starts after a delay and optionally runs periodically.
///
/// @param <V> the type of the task result
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
final class VirtualThreadScheduledFutureTask<V> extends ManagedVirtualThread<V> {

    private static final AtomicInteger differentiatorGenerator = new AtomicInteger();

    @SuppressWarnings("rawtypes")
    private static final AtomicIntegerFieldUpdater<VirtualThreadScheduledFutureTask> differentiatorUpdater = AtomicIntegerFieldUpdater
            .newUpdater(VirtualThreadScheduledFutureTask.class, "differentiator");

    /// If positive, the task is run at fixed rate. If negative, the task is run with fixed delay.
    /// If zero, the task is not periodic.
    private final long periodNanos;
    private long deadlineNanos;

    /// A value that is used as the last resort for comparison in [#compareTo(Delayed)].
    /// This field is set lazily only when [#compareTo(Delayed)] finds two objects with the same
    /// [System#identityHashCode(Object)identityhashcode]. `0` means that this field is not set yet.
    ///
    /// @see ThreadPoolUtil#compareScheduledTasks(Object, long, Object, long, AtomicIntegerFieldUpdater, AtomicInteger)
    @SuppressWarnings("unused")
    private volatile int differentiator;

    VirtualThreadScheduledFutureTask(VirtualThreadPool parent,
                                     Runnable task, @Nullable V result,
                                     long initialDelayNanos, long periodNanos) {
        super(parent, Executors.callable(task, result));
        deadlineNanos = System.nanoTime() + initialDelayNanos;
        this.periodNanos = periodNanos;
        // A negative periodNanos value is always negated from a positive long value.
        assert periodNanos != Long.MIN_VALUE;
    }

    VirtualThreadScheduledFutureTask(VirtualThreadPool parent, Callable<V> task, long delayNanos) {
        super(parent, task);
        deadlineNanos = System.nanoTime() + delayNanos;
        periodNanos = 0; // There are no periodic `schedule*()` methods that accept a `Callable`.
    }

    @Override
    @Nullable
    protected V call() throws Exception {
        long delayNanos = getDelay(TimeUnit.NANOSECONDS);
        if (isPeriodic()) {
            for (; ; ) {
                if (isDone()) {
                    // The task was cancelled or the pool was shut down.
                    break;
                }
                delay(delayNanos);
                if (isDone()) {
                    break;
                }
                super.call();
                delayNanos = updateDeadlineAndGetDelayNanos();
            }
            return null;
        } else {
            delay(delayNanos);
            // The task may have been cancelled or the pool may have been shut down while waiting.
            if (isDone() || !setProcessed()) {
                return null;
            }
            return super.call();
        }
    }

    private static void delay(long delayNanos) throws InterruptedException {
        if (delayNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(delayNanos);
        }
    }

    @Override
    public boolean isPeriodic() {
        return periodNanos != 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(deadlineNanos - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    long updateDeadlineAndGetDelayNanos() {
        final long currentTimeNanos = System.nanoTime();
        if (periodNanos > 0) { // Run at fixed rate.
            deadlineNanos += periodNanos;
            return deadlineNanos - currentTimeNanos;
        } else { // Run with fixed delay.
            final long delayNanos = -periodNanos;
            // periodNanos is negated from a positive long value, so it is never
            // Long.MIN_VALUE.
            assert delayNanos > 0;
            deadlineNanos = currentTimeNanos + delayNanos;
            return delayNanos;
        }
    }

    @Override
    public int compareTo(Delayed o) {
        if (o == this) {
            return 0;
        }

        if (getClass() != o.getClass()) {
            throw new IllegalArgumentException(
                    "cannot compare two different Delayed instances: " +
                            getClass().getName() + " vs. " +
                            o.getClass().getName());
        }

        final VirtualThreadScheduledFutureTask<?> that = (VirtualThreadScheduledFutureTask<?>) o;
        return ThreadPoolUtil.compareScheduledTasks(this, deadlineNanos, that, that.deadlineNanos,
                differentiatorUpdater, differentiatorGenerator);
    }
}
