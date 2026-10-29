package io.github.tuyucheng777.fastthreadpool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/// A [ScheduledFuture] that is also a [CompletableFuture].
///
/// Every scheduling method of [ThreadPool] returns this type, so the result of a scheduled task can be
/// composed with the usual [CompletableFuture] methods, such as
/// [CompletableFuture#thenApply(java.util.function.Function)] or
/// [CompletableFuture#exceptionally(java.util.function.Function)].
///
/// A periodic task completes only when it is cancelled, or when the pool is shut down; a one-shot task
/// completes when it has finished or has failed.
///
/// @param <V> the type of the task result
public abstract class CompletableScheduledFuture<V> extends CompletableFuture<V> implements ScheduledFuture<V> {
}
