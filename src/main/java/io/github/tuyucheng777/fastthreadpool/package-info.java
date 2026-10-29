/// A [ScheduledExecutorService](java.util.concurrent.ScheduledExecutorService) implementation backed by
/// platform threads or virtual threads.
///
/// The entry point is [ThreadPool]:
///
/// ```java
/// ThreadPool platformPool = ThreadPool.builder(8).build();
/// ThreadPool virtualPool = ThreadPool.ofVirtual();
/// ```
///
/// Tasks can be submitted with `execute()`, `submit()`, and the `schedule*()` methods. Platform-thread
/// pools can grow and shrink on demand, interrupt tasks that exceed a timeout, and export Micrometer
/// metrics; virtual-thread pools start one virtual thread per task.
///
/// See [ThreadPoolBuilder] and [VirtualThreadPoolBuilder] for the available configuration options.
package io.github.tuyucheng777.fastthreadpool;
