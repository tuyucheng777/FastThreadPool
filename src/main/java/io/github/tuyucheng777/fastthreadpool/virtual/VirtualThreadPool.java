package io.github.tuyucheng777.fastthreadpool.virtual;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import io.github.tuyucheng777.fastthreadpool.internal.AbstractThreadPool;
import io.github.tuyucheng777.fastthreadpool.internal.ManagedThread;
import io.github.tuyucheng777.fastthreadpool.handler.TaskAction;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.ThreadPoolMetrics;
import io.github.tuyucheng777.fastthreadpool.CompletableScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolUtil.validateDelay;
import static io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolUtil.validatePeriod;
import static io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolUtil.handleLateSubmission;
import static io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolUtil.invokeExceptionHandler;
import static java.util.Objects.requireNonNull;

/// A [ThreadPool] that starts a virtual thread for every task.
///
/// Scheduling is implemented by delaying inside the virtual thread with
/// [TimeUnit#sleep(long)], which is cheap because virtual threads are unmounted while sleeping.
/// Metrics are not supported yet, so [ThreadPool#metrics()] returns `null`.
public final class VirtualThreadPool extends AbstractThreadPool {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadPool.class);

    static final ThreadFactory threadFactory = Thread.ofVirtual().factory();

    // TaskExceptionHandler and metrics are not supported for virtual threads yet.

    private final TaskSubmissionHandler submissionHandler;
    private final TaskExceptionHandler exceptionHandler;

    private final Set<ManagedVirtualThread<?>> tasks = ConcurrentHashMap.newKeySet();
    private final Set<ManagedVirtualThread<?>> activeTasks = ConcurrentHashMap.newKeySet();

    public VirtualThreadPool(TaskSubmissionHandler submissionHandler,
                      TaskExceptionHandler exceptionHandler,
                      long taskTimeoutNanos, long watchdogIntervalNanos,
                      MeterRegistry meterRegistry, @Nullable String metricPrefix, List<Tag> metricTags) {
        super(taskTimeoutNanos, watchdogIntervalNanos);
        this.submissionHandler = submissionHandler;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public boolean isVirtual() {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        requireNonNull(command, "command");
        if (!handleLateSubmission(this, submissionHandler, command)) {
            return;
        }

        handleSubmission(command);
    }

    private void handleSubmission(Runnable task) {
        final TaskAction action = submissionHandler.handleSubmission(task, this);
        if (action == TaskAction.accept()) {
            final ManagedVirtualThread<?> wrappedTask;
            final Class<?> taskType = task.getClass();
            if (taskType == VirtualThreadFutureTask.class ||
                    taskType == VirtualThreadScheduledFutureTask.class) {
                // Wrapped already by methods like `submit()` or `schedule()`.
                wrappedTask = (ManagedVirtualThread<?>) task;
            } else {
                // Wrap the task so we can measure metrics.
                wrappedTask = newTaskFor(task, null);
            }

            final Watchdog watchdog = watchdog();
            if (watchdog != null) {
                watchdog.start();
            }

            wrappedTask.start();
        } else {
            action.doAction(task);
        }
    }

    @Override
    protected <T> ManagedVirtualThread<T> newTaskFor(Runnable runnable, @Nullable T value) {
        return new VirtualThreadFutureTask<>(this, runnable, value);
    }

    @Override
    protected <T> ManagedVirtualThread<T> newTaskFor(Callable<T> callable) {
        return new VirtualThreadFutureTask<>(this, callable);
    }

    @Override
    public CompletableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        requireNonNull(command, "command");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<?> task =
                new VirtualThreadScheduledFutureTask<>(this, command, null, delayNanos, 0);
        execute(task);
        return task;
    }

    @Override
    public <V> CompletableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        requireNonNull(callable, "callable");
        final long delayNanos = validateDelay(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<V> task =
                new VirtualThreadScheduledFutureTask<>(this, callable, delayNanos);
        execute(task);
        return task;
    }

    @Override
    public CompletableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
                                                             TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long periodNanos = validatePeriod(period, unit, "period");
        final VirtualThreadScheduledFutureTask<Object> task =
                new VirtualThreadScheduledFutureTask<>(this, command, null, initialDelayNanos, periodNanos);
        execute(task);
        return task;
    }

    @Override
    public CompletableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
                                                                TimeUnit unit) {
        requireNonNull(command, "command");
        final long initialDelayNanos = validateDelay(initialDelay, unit, "initialDelay");
        final long delayNanos = validatePeriod(delay, unit, "delay");
        final VirtualThreadScheduledFutureTask<Object> task =
                new VirtualThreadScheduledFutureTask<>(this, command, null, initialDelayNanos, -delayNanos);
        execute(task);
        return task;
    }

    void addTask(ManagedVirtualThread<?> task) {
        tasks.add(task);
    }

    void removeTask(ManagedVirtualThread<?> task) {
        tasks.remove(task);
    }

    void addActiveTask(ManagedVirtualThread<?> task) {
        activeTasks.add(task);
    }

    void removeActiveTask(ManagedVirtualThread<?> task) {
        activeTasks.remove(task);
    }

    void invokeTaskException(Callable<?> task, Throwable cause) {
        try {
            exceptionHandler.handleTaskException(task, cause, this);
        } catch (Throwable handlerCause) {
            handlerCause.addSuppressed(cause);
            logger.warn("Unexpected exception from a task exception handler:", handlerCause);
        }
    }

    @Nullable
    @Override
    public ThreadPoolMetrics metrics() {
        return null;
    }

    @Override
    public List<Runnable> shutdownNow() {
        final List<Runnable> unprocessed = new ArrayList<>();
        doShutdown(true, unprocessed);
        return unprocessed;
    }

    @Override
    protected boolean hasManagedThreads() {
        return !tasks.isEmpty();
    }

    @Override
    protected void forEachManagedThread(Consumer<ManagedThread<?>> block) {
        tasks.forEach(block);
    }

    @Override
    protected void forEachActiveManagedThread(Consumer<ManagedThread<?>> block) {
        activeTasks.forEach(block);
    }

    @Override
    protected void sendPoisonPills() {
    }
}
