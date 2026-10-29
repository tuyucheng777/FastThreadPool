package io.github.tuyucheng777.fastthreadpool.internal;

import io.github.tuyucheng777.fastthreadpool.ThreadPool;

/// The lifecycle states of a [ThreadPool].
///
/// `SHUT_DOWN_WITHOUT_INTERRUPTS` and `SHUT_DOWN_WITH_INTERRUPTS` both mean that the pool no longer
/// accepts tasks; the latter additionally interrupts the running tasks. `TERMINATED` means that all
/// managed threads have stopped.
public enum ThreadPoolState {
    STARTED,
    SHUT_DOWN_WITHOUT_INTERRUPTS,
    SHUT_DOWN_WITH_INTERRUPTS,
    TERMINATED
}
