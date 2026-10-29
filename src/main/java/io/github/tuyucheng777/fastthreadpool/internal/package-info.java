/// Internal engine shared by the platform-thread and virtual-thread pools.
///
/// [AbstractThreadPool] implements the shutdown and termination state machine, [ManagedThread] wraps a
/// JDK thread with lifecycle tracking, and [ThreadPoolUtil] contains helpers used by both
/// implementations. These types are public only because the implementation packages are siblings;
/// application code should not depend on them.
package io.github.tuyucheng777.fastthreadpool.internal;
