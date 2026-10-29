package io.github.tuyucheng777.fastthreadpool.internal;

import org.slf4j.Logger;
import javax.annotation.Nullable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import io.github.tuyucheng777.fastthreadpool.handler.TaskAction;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/// Internal helpers shared by the thread pool implementations.
///
/// The members of this class are public only because the implementation classes live in sibling
/// packages. Application code should not depend on this class.
public final class ThreadPoolUtil {

    /// The factory used for every platform thread created by the pool.
    public static final ThreadFactory threadFactory = Executors.defaultThreadFactory();

    public static long validateDelay(long delay, TimeUnit unit, String name) {
        requireNonNull(unit, "unit");

        final long delayNanos = unit.toNanos(delay);
        if (delayNanos < 0 || delayNanos >= (Long.MAX_VALUE / 2)) {
            // e.g. "initialDelay: -42 DAYS (expected: 0 <= initialDelay < 146 years)"
            throw new IllegalArgumentException(
                    name + ": " + delay + ' ' + unit +
                            " (expected: 0 <= " + name + " < 146 years)");
        }

        return delayNanos;
    }

    /// Validates a positive period or delay used by the periodic scheduling methods.
    public static long validatePeriod(long period, TimeUnit unit, String name) {
        final long periodNanos = validateDelay(period, unit, name);
        checkArgument(periodNanos > 0, "%s: %s (expected: > 0)", name, period);
        return periodNanos;
    }

    public static boolean handleLateSubmission(ThreadPool threadPool, TaskSubmissionHandler submissionHandler, Runnable task) {
        if (!threadPool.isShutdown()) {
            return true;
        }

        final TaskAction action = submissionHandler.handleLateSubmission(task, threadPool);
        checkState(action != TaskAction.accept(),
                "TaskSubmissionHandler.handleLateSubmission() should never accept a task.");
        action.doAction(task);
        return false;
    }

    public static void invokeExceptionHandler(ThreadPool threadPool, TaskExceptionHandler exceptionHandler,
                                       @Nullable Runnable task, Throwable cause, Logger logger) {

        if (task == null) {
            logger.warn("Unexpected exception from a thread pool:", cause);
            return;
        }

        try {
            exceptionHandler.handleTaskException(task, cause, threadPool);
        } catch (Throwable handlerCause) {
            handlerCause.addSuppressed(cause);
            logger.warn("Unexpected exception from a task exception handler:", handlerCause);
        }
    }

    public static <T> int compareScheduledTasks(T a,
                                         long deadlineNanosA,
                                         T b,
                                         long deadlineNanosB,
                                         AtomicIntegerFieldUpdater<T> differentiatorUpdater,
                                         AtomicInteger differentiatorGenerator) {

        final long diff = deadlineNanosA - deadlineNanosB;
        if (diff < 0) {
            return -1;
        }
        if (diff > 0) {
            return 1;
        }

        final int thisId = System.identityHashCode(a);
        final int thatId = System.identityHashCode(b);
        if (thisId < thatId) {
            return -1;
        }
        if (thisId > thatId) {
            return 1;
        }

        final int thisDifferentiator = differentiator(a, differentiatorUpdater, differentiatorGenerator);
        final int thatDifferentiator = differentiator(b, differentiatorUpdater, differentiatorGenerator);
        if (thisDifferentiator < thatDifferentiator) {
            return -1;
        }
        if (thisDifferentiator > thatDifferentiator) {
            return 1;
        }

        throw new AssertionError("Can't return 0 for two different objects");
    }

    private static <T> int differentiator(T task,
                                          AtomicIntegerFieldUpdater<T> updater,
                                          AtomicInteger generator) {
        for (; ; ) {
            final int oldDifferentiator = updater.get(task);
            if (oldDifferentiator != 0) {
                return oldDifferentiator;
            }

            final int newDifferentiator = generator.incrementAndGet();
            if (newDifferentiator == 0) {
                continue;
            }

            if (!updater.compareAndSet(task, 0, newDifferentiator)) {
                continue;
            }

            return newDifferentiator;
        }
    }

    private ThreadPoolUtil() {
    }
}
