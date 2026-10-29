/// Platform-thread implementation of [io.github.tuyucheng777.fastthreadpool.ThreadPool].
///
/// [PlatformThreadPool] owns a growable [PlatformWorkerGroup] for regular tasks and a min-heap
/// scheduler for delayed and periodic tasks. Most applications should create a platform-thread pool
/// through [io.github.tuyucheng777.fastthreadpool.ThreadPool#builder(int)] instead of using these
/// classes directly.
package io.github.tuyucheng777.fastthreadpool.platform;
