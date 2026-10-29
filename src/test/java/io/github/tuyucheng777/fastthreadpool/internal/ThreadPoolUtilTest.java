package io.github.tuyucheng777.fastthreadpool.internal;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import io.github.tuyucheng777.fastthreadpool.handler.TaskAction;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.TestSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolUtilTest {

   private static final Logger logger = LoggerFactory.getLogger(ThreadPoolUtilTest.class);

   @Test
   void compareScheduledTasksOrdersByDeadline() {
      final ScheduledTask earlier = new ScheduledTask();
      final ScheduledTask later = new ScheduledTask();
      assertEquals(-1, ThreadPoolUtil.compareScheduledTasks(
            earlier, 1, later, 2, ScheduledTask.UPDATER, new AtomicInteger()));
      assertEquals(1, ThreadPoolUtil.compareScheduledTasks(
            later, 2, earlier, 1, ScheduledTask.UPDATER, new AtomicInteger()));
   }

   @Test
   void compareScheduledTasksBreaksTiesWithAStableOrder() {
      final ScheduledTask first = new ScheduledTask();
      final ScheduledTask second = new ScheduledTask();
      final AtomicInteger generator = new AtomicInteger();
      final int result = ThreadPoolUtil.compareScheduledTasks(
            first, 1, second, 1, ScheduledTask.UPDATER, generator);
      final int reversed = ThreadPoolUtil.compareScheduledTasks(
            second, 1, first, 1, ScheduledTask.UPDATER, generator);
      assertNotEquals(0, result);
      assertEquals(-result, reversed);
   }

   @Test
   void handleLateSubmissionReturnsTrueWhileRunning() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertTrue(ThreadPoolUtil.handleLateSubmission(
               pool, TaskSubmissionHandler.ofDefault(), () -> {
               }));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void handleLateSubmissionRejectsTasksAfterShutdown() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      pool.shutdown();
      try {
         assertThrows(RejectedExecutionException.class, () -> ThreadPoolUtil.handleLateSubmission(
               pool, TaskSubmissionHandler.ofDefault(), () -> {
               }));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void handleLateSubmissionCanLogInsteadOfRejecting() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      pool.shutdown();
      try {
         final AtomicBoolean attempted = new AtomicBoolean();
         final TaskSubmissionHandler handler = new TaskSubmissionHandler() {
            @Override
            public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
               return TaskAction.accept();
            }

            @Override
            public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
               return TaskAction.accept();
            }

            @Override
            public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
               return TaskAction.log();
            }

            @Override
            public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
               return TaskAction.log();
            }
         };
         assertFalse(ThreadPoolUtil.handleLateSubmission(pool, handler, () -> attempted.set(true)));
         assertFalse(attempted.get());
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void handleLateSubmissionRejectsHandlersThatAcceptLateTasks() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      pool.shutdown();
      try {
         final TaskSubmissionHandler handler = new TaskSubmissionHandler() {
            @Override
            public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
               return TaskAction.accept();
            }

            @Override
            public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
               return TaskAction.accept();
            }

            @Override
            public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
               return TaskAction.accept();
            }

            @Override
            public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
               return TaskAction.accept();
            }
         };
         assertThrows(IllegalStateException.class,
               () -> ThreadPoolUtil.handleLateSubmission(pool, handler, () -> {
               }));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void invokeExceptionHandlerHandlesNullTasks() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertDoesNotThrow(() -> ThreadPoolUtil.invokeExceptionHandler(
               pool, TaskExceptionHandler.ofDefault(), null,
               new IllegalStateException("boom"), logger));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void invokeExceptionHandlerSwallowsHandlerFailures() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final TaskExceptionHandler handler = new TaskExceptionHandler() {
            @Override
            public void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool) {
               throw new IllegalStateException("handler failure");
            }

            @Override
            public void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool) {
               throw new IllegalStateException("handler failure");
            }
         };
         assertDoesNotThrow(() -> ThreadPoolUtil.invokeExceptionHandler(
               pool, handler, () -> {
               }, new IllegalArgumentException("task failure"), logger));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void invokeExceptionHandlerDelegatesToTheHandler() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final AtomicInteger handled = new AtomicInteger();
         final TaskExceptionHandler handler = new TaskExceptionHandler() {
            @Override
            public void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool) {
               assertSame(pool, threadPool);
               handled.incrementAndGet();
            }

            @Override
            public void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool) {
               handled.incrementAndGet();
            }
         };
         ThreadPoolUtil.invokeExceptionHandler(pool, handler, () -> {
         }, new IllegalArgumentException("task failure"), logger);
         assertEquals(1, handled.get());
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   private static final class ScheduledTask {

      static final AtomicIntegerFieldUpdater<ScheduledTask> UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ScheduledTask.class, "differentiator");

      @SuppressWarnings("unused")
      private volatile int differentiator;
   }
}
