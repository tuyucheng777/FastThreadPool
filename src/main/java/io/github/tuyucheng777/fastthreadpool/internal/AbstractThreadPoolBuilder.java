package io.github.tuyucheng777.fastthreadpool.internal;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.ThreadPoolBuilder;
import io.github.tuyucheng777.fastthreadpool.VirtualThreadPoolBuilder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// The configuration shared by [ThreadPoolBuilder] and [VirtualThreadPoolBuilder].
///
/// @param <SELF> the concrete builder type, so that configuration methods return the right subtype
public abstract class AbstractThreadPoolBuilder<SELF extends AbstractThreadPoolBuilder<SELF>> {

    private static final long DEFAULT_WATCHDOG_INTERVAL_SECONDS = 1;

    private TaskSubmissionHandler submissionHandler = TaskSubmissionHandler.ofDefault();
    private TaskExceptionHandler exceptionHandler = TaskExceptionHandler.ofDefault();
    private long taskTimeoutNanos; // 0 means 'disabled'.
    private long watchdogIntervalNanos = TimeUnit.SECONDS.toNanos(DEFAULT_WATCHDOG_INTERVAL_SECONDS);
    private MeterRegistry meterRegistry = Metrics.globalRegistry;
    @Nullable
    private String metricPrefix;
    private List<Tag> metricTags = ImmutableList.of();

    @SuppressWarnings("unchecked")
    private SELF self() {
        return (SELF) this;
    }

    /// Sets the maximum time a task may run before the watchdog interrupts its worker.
    ///
    /// The default is `0`, which disables the watchdog. A task that is interrupted is expected to stop
    /// promptly; the pool does not force it to terminate.
    ///
    /// @param taskTimeout the maximum task execution time, which must not be negative
    /// @param unit        the time unit of `taskTimeout`
    /// @return this builder
    public final SELF taskTimeout(long taskTimeout, TimeUnit unit) {
        checkArgument(taskTimeout >= 0, "taskTimeout: %s (expected: >= 0)", taskTimeout);
        taskTimeoutNanos = requireNonNull(unit, "unit").toNanos(taskTimeout);
        return self();
    }

    /// Sets the maximum time a task may run before the watchdog interrupts its worker.
    ///
    /// @param taskTimeout the maximum task execution time, which must not be negative
    /// @return this builder
    public final SELF taskTimeout(Duration taskTimeout) {
        requireNonNull(taskTimeout, "taskTimeout");
        checkArgument(!taskTimeout.isNegative(), "taskTimeout: %s (expected: >= 0)", taskTimeout);
        return taskTimeout(taskTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Sets how often the watchdog checks for tasks that exceeded the task timeout.
    ///
    /// The default is 1 second. This method has no effect when [the task timeout](#taskTimeout(long,java.util.concurrent.TimeUnit))
    /// is disabled.
    ///
    /// @param watchdogInterval the watchdog interval, which must be greater than `0`
    /// @param unit             the time unit of `watchdogInterval`
    /// @return this builder
    public final SELF watchdogInterval(long watchdogInterval, TimeUnit unit) {
        checkArgument(watchdogInterval > 0, "watchdogInterval: %s (expected: > 0)", watchdogInterval);
        watchdogIntervalNanos = requireNonNull(unit, "unit").toNanos(watchdogInterval);
        return self();
    }

    /// Sets how often the watchdog checks for tasks that exceeded the task timeout.
    ///
    /// @param watchdogInterval the watchdog interval, which must be greater than zero
    /// @return this builder
    public final SELF watchdogInterval(Duration watchdogInterval) {
        requireNonNull(watchdogInterval, "watchdogInterval");
        checkArgument(!watchdogInterval.isZero() &&
                        !watchdogInterval.isNegative(),
                "watchdogInterval: %s (expected: > 0)", watchdogInterval);
        return watchdogInterval(watchdogInterval.toNanos(), TimeUnit.NANOSECONDS);
    }

    /// Sets the handler that decides whether submitted tasks are accepted.
    ///
    /// The default is [TaskSubmissionHandler#ofDefault()].
    ///
    /// @param submissionHandler the submission handler
    /// @return this builder
    public final SELF submissionHandler(TaskSubmissionHandler submissionHandler) {
        this.submissionHandler = requireNonNull(submissionHandler, "submissionHandler");
        return self();
    }

    /// Sets the handler that receives exceptions escaping from tasks.
    ///
    /// The default is [TaskExceptionHandler#ofDefault()].
    ///
    /// @param exceptionHandler the task exception handler
    /// @return this builder
    public final SELF exceptionHandler(TaskExceptionHandler exceptionHandler) {
        this.exceptionHandler = requireNonNull(exceptionHandler, "exceptionHandler");
        return self();
    }

    /// Enables Micrometer metrics in the global registry.
    ///
    /// @param metricPrefix the prefix of every registered meter
    /// @param metricTags   the tags attached to every registered meter
    /// @return this builder
    public final SELF metrics(String metricPrefix, Iterable<? extends Tag> metricTags) {
        return metrics(Metrics.globalRegistry, metricPrefix, metricTags);
    }

    /// Enables Micrometer metrics.
    ///
    /// @param meterRegistry the registry to register meters with
    /// @param metricPrefix  the prefix of every registered meter
    /// @param metricTags    the tags attached to every registered meter
    /// @return this builder
    public final SELF metrics(MeterRegistry meterRegistry, String metricPrefix,
                              Tag... metricTags) {
        return metrics(meterRegistry, metricPrefix,
                ImmutableList.copyOf(requireNonNull(metricTags, "metricTags")));
    }

    /// Enables Micrometer metrics.
    ///
    /// @param meterRegistry the registry to register meters with
    /// @param metricPrefix  the prefix of every registered meter
    /// @param metricTags    the tags attached to every registered meter
    /// @return this builder
    public final SELF metrics(MeterRegistry meterRegistry, String metricPrefix,
                              Iterable<? extends Tag> metricTags) {
        requireNonNull(meterRegistry, "meterRegistry");
        requireNonNull(metricPrefix, "metricPrefix");
        requireNonNull(metricTags, "metricTags");
        this.meterRegistry = meterRegistry;
        this.metricPrefix = metricPrefix;
        this.metricTags = ImmutableList.copyOf(metricTags);
        return self();
    }

    /// Builds the configured [ThreadPool].
    ///
    /// @return a new pool that is not shut down
    public final ThreadPool build() {
        return build(submissionHandler, exceptionHandler,
                taskTimeoutNanos, watchdogIntervalNanos,
                meterRegistry, metricPrefix, metricTags);
    }

    protected abstract ThreadPool build(TaskSubmissionHandler submissionHandler, TaskExceptionHandler exceptionHandler,
                              long taskTimeoutNanos, long watchdogIntervalNanos,
                              MeterRegistry meterRegistry, @Nullable String metricPrefix, List<Tag> metricTags);
}
