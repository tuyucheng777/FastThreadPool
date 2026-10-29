package io.github.tuyucheng777.fastthreadpool.platform;

import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.github.tuyucheng777.fastthreadpool.TestSupport.shutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformThreadFutureTaskTest {

   @Test
   void platformFutureTaskCompletesAndRecordsWaitTime() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final Timer timer = registry.timer("wait");
      final PlatformThreadFutureTask<String> task = new PlatformThreadFutureTask<>(
            (Callable<String>) () -> "result", timer);

      task.run();
      assertEquals("result", task.get(10, TimeUnit.SECONDS));
      assertEquals(1, timer.count());

      // Running a completed task again is a no-op.
      task.run();
      assertEquals(1, timer.count());
   }

   @Test
   void platformFutureTaskFailureCompletesExceptionally() {
      final PlatformThreadFutureTask<String> task = new PlatformThreadFutureTask<>(
            (Callable<String>) () -> {
               throw new IllegalArgumentException("boom");
            }, null);

      task.run();
      final ExecutionException cause = assertThrows(ExecutionException.class,
            () -> task.get(10, TimeUnit.SECONDS));
      assertInstanceOf(IllegalArgumentException.class, cause.getCause());
   }

   @Test
   void platformFutureTaskCancellationRecordsWaitTime() {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final Timer timer = registry.timer("wait");
      final PlatformThreadFutureTask<String> task = new PlatformThreadFutureTask<>(
            (Callable<String>) () -> "result", timer);

      assertTrue(task.cancel(false));
      assertEquals(1, timer.count());
      assertTrue(task.isCancelled());
   }

   @Test
   void platformFutureTaskSupportsRunnableTasks() throws Exception {
      final PlatformThreadFutureTask<String> task = new PlatformThreadFutureTask<>(
            () -> {
            }, "result", null);
      task.run();
      assertEquals("result", task.get(10, TimeUnit.SECONDS));
   }

   @Test
   void platformFutureTaskRejectsExternalCompletion() {
      final PlatformThreadFutureTask<String> task = new PlatformThreadFutureTask<>(
            (Callable<String>) () -> "result", null);
      assertThrows(UnsupportedOperationException.class, () -> task.complete("value"));
      assertThrows(UnsupportedOperationException.class,
            () -> task.completeExceptionally(new IllegalStateException()));
      assertThrows(UnsupportedOperationException.class, () -> task.obtrudeValue("value"));
      assertThrows(UnsupportedOperationException.class,
            () -> task.obtrudeException(new IllegalStateException()));
   }

   @Test
   void platformScheduledFutureTasksCompareAndExposeDelays() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final PlatformThreadScheduledFutureTask<?> earlier = (PlatformThreadScheduledFutureTask<?>)
               pool.schedule(() -> {
               }, 10, TimeUnit.MILLISECONDS);
         final PlatformThreadScheduledFutureTask<?> later = (PlatformThreadScheduledFutureTask<?>)
               pool.schedule(() -> {
               }, 100, TimeUnit.MILLISECONDS);

         assertEquals(0, earlier.compareTo(earlier));
         assertEquals(-1, earlier.compareTo(later));
         assertEquals(1, later.compareTo(earlier));
         assertThrows(IllegalArgumentException.class, () -> earlier.compareTo(delayed()));
         assertTrue(earlier.getDelay(TimeUnit.NANOSECONDS) > 0);
         assertFalse(earlier.isPeriodic());

         assertTrue(earlier.cancel(false));
         assertTrue(later.cancel(false));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void platformPeriodicTaskIsScheduled() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         final PlatformThreadScheduledFutureTask<?> task = (PlatformThreadScheduledFutureTask<?>)
               pool.scheduleAtFixedRate(latch::countDown, 0, 10, TimeUnit.MILLISECONDS);
         assertTrue(task.isPeriodic());
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertTrue(task.cancel(false));
      } finally {
         shutdown(pool);
      }
   }

   private static Delayed delayed() {
      return new Delayed() {
         @Override
         public long getDelay(TimeUnit unit) {
            return 0;
         }

         @Override
         public int compareTo(Delayed o) {
            return 0;
         }
      };
   }
}
