package io.github.tuyucheng777.fastthreadpool;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import javax.annotation.Nullable;
import java.util.List;
import io.github.tuyucheng777.fastthreadpool.internal.AbstractThreadPoolBuilder;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.virtual.VirtualThreadPool;

/// Builds a virtual-thread [ThreadPool].
///
/// A builder is obtained from [ThreadPool#builderForVirtual()]. The resulting pool starts one virtual
/// thread per task and supports the same submission and scheduling methods as a platform-thread pool:
///
/// ```java
/// ThreadPool pool = ThreadPool.builderForVirtual()
///         .taskTimeout(Duration.ofSeconds(30))
///         .build();
/// pool.submit(() -> "hello").thenAccept(System.out::println);
/// ```
///
/// Virtual-thread pools do not export metrics, so [ThreadPool#metrics()] returns `null`. A builder is
/// not thread-safe and should be used from a single thread.
///
/// @see ThreadPool
public final class VirtualThreadPoolBuilder extends AbstractThreadPoolBuilder<VirtualThreadPoolBuilder> {

    VirtualThreadPoolBuilder() {
    }

    @Override
    protected ThreadPool build(TaskSubmissionHandler submissionHandler, TaskExceptionHandler exceptionHandler,
                     long taskTimeoutNanos, long watchdogIntervalNanos, MeterRegistry meterRegistry,
                     @Nullable String metricPrefix, List<Tag> metricTags) {

        return new VirtualThreadPool(submissionHandler, exceptionHandler,
                taskTimeoutNanos, watchdogIntervalNanos,
                meterRegistry, metricPrefix, metricTags);
    }
}
