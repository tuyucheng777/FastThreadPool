/// Task admission and exception handling for [io.github.tuyucheng777.fastthreadpool.ThreadPool].
///
/// [TaskSubmissionHandler] decides whether a submitted task is accepted, rejected, or dropped, and
/// [TaskExceptionHandler] receives exceptions that escape from tasks. [TaskAction] describes the
/// outcome of an admission decision.
package io.github.tuyucheng777.fastthreadpool.handler;
