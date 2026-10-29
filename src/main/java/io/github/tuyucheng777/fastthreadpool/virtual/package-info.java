/// Virtual-thread implementation of [io.github.tuyucheng777.fastthreadpool.ThreadPool].
///
/// [VirtualThreadPool] starts one virtual thread per task and implements delayed execution by sleeping
/// inside that virtual thread. Most applications should create a virtual-thread pool through
/// [io.github.tuyucheng777.fastthreadpool.ThreadPool#ofVirtual()] instead of using these classes
/// directly.
package io.github.tuyucheng777.fastthreadpool.virtual;
