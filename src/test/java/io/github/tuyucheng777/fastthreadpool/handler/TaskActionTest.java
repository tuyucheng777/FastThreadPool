package io.github.tuyucheng777.fastthreadpool.handler;

import org.junit.jupiter.api.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.TestSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TaskActionTest {

   private static final Runnable RUNNABLE = () -> {
   };
   private static final Callable<String> CALLABLE = () -> "value";

   @Test
   void acceptDoesNothing() {
      assertDoesNotThrow(() -> TaskAction.accept().doAction(RUNNABLE));
      assertDoesNotThrow(() -> TaskAction.accept().doAction(CALLABLE));
   }

   @Test
   void rejectThrowsRejectedExecutionException() {
      assertThrows(RejectedExecutionException.class, () -> TaskAction.reject().doAction(RUNNABLE));
      assertThrows(RejectedExecutionException.class, () -> TaskAction.reject().doAction(CALLABLE));
   }

   @Test
   void logDoesNotThrow() {
      assertDoesNotThrow(() -> TaskAction.log().doAction(RUNNABLE));
      assertDoesNotThrow(() -> TaskAction.log().doAction(CALLABLE));
   }

   @Test
   void defaultSubmissionHandlerAcceptsRunningPoolAndRejectsLateTasks() throws Exception {
      final TaskSubmissionHandler handler = TaskSubmissionHandler.ofDefault();
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertSame(TaskAction.accept(), handler.handleSubmission(RUNNABLE, pool));
         assertSame(TaskAction.accept(), handler.handleSubmission(CALLABLE, pool));
         assertSame(TaskAction.reject(), handler.handleLateSubmission(RUNNABLE, pool));
         assertSame(TaskAction.reject(), handler.handleLateSubmission(CALLABLE, pool));
      } finally {
         TestSupport.shutdown(pool);
      }
   }

   @Test
   void defaultExceptionHandlerLogsFailures() throws Exception {
      final TaskExceptionHandler handler = TaskExceptionHandler.ofDefault();
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertDoesNotThrow(() -> handler.handleTaskException(RUNNABLE, new IllegalStateException("runnable"), pool));
         assertDoesNotThrow(() -> handler.handleTaskException(CALLABLE, new IllegalStateException("callable"), pool));
      } finally {
         TestSupport.shutdown(pool);
      }
   }
}
