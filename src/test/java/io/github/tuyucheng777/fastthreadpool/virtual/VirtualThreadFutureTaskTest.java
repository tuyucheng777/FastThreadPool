package io.github.tuyucheng777.fastthreadpool.virtual;

import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import org.junit.jupiter.api.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.tuyucheng777.fastthreadpool.TestSupport.await;
import static io.github.tuyucheng777.fastthreadpool.TestSupport.shutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VirtualThreadFutureTaskTest {

   @Test
   void virtualFutureTaskCompletes() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final VirtualThreadFutureTask<String> task = new VirtualThreadFutureTask<>(
               (VirtualThreadPool) pool, () -> "virtual");
         task.start();
         assertEquals("virtual", task.get(10, TimeUnit.SECONDS));
         task.joinThread();
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void virtualFutureTaskSupportsRunnableTasks() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final VirtualThreadFutureTask<String> task = new VirtualThreadFutureTask<>(
               (VirtualThreadPool) pool,
               () -> {
               }, "virtual");
         task.start();
         assertEquals("virtual", task.get(10, TimeUnit.SECONDS));
         task.joinThread();
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void virtualScheduledFutureTaskRunsAfterItsDelay() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final VirtualThreadScheduledFutureTask<String> task = new VirtualThreadScheduledFutureTask<>(
               (VirtualThreadPool) pool, (Callable<String>) () -> "scheduled",
               TimeUnit.MILLISECONDS.toNanos(20));
         assertFalse(task.isPeriodic());
         assertTrue(task.getDelay(TimeUnit.NANOSECONDS) > 0);
         task.start();
         assertEquals("scheduled", task.get(10, TimeUnit.SECONDS));
         task.joinThread();
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void virtualScheduledFutureTaskComputesPeriodicDelays() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final VirtualThreadScheduledFutureTask<Void> fixedRate = new VirtualThreadScheduledFutureTask<Void>(
               (VirtualThreadPool) pool, () -> {
               }, null, 0, TimeUnit.MILLISECONDS.toNanos(10));
         assertTrue(fixedRate.isPeriodic());
         assertTrue(fixedRate.updateDeadlineAndGetDelayNanos() > 0);

         final VirtualThreadScheduledFutureTask<Void> fixedDelay = new VirtualThreadScheduledFutureTask<Void>(
               (VirtualThreadPool) pool, () -> {
               }, null, 0, -TimeUnit.MILLISECONDS.toNanos(10));
         assertTrue(fixedDelay.isPeriodic());
         assertEquals(TimeUnit.MILLISECONDS.toNanos(10), fixedDelay.updateDeadlineAndGetDelayNanos());

         assertTrue(fixedRate.cancel(false));
         assertTrue(fixedDelay.cancel(false));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void virtualScheduledFutureTaskComparesByDeadline() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final VirtualThreadScheduledFutureTask<Void> earlier = new VirtualThreadScheduledFutureTask<Void>(
               (VirtualThreadPool) pool, () -> {
               }, null, TimeUnit.MILLISECONDS.toNanos(10), 0);
         final VirtualThreadScheduledFutureTask<Void> later = new VirtualThreadScheduledFutureTask<Void>(
               (VirtualThreadPool) pool, () -> {
               }, null, TimeUnit.MILLISECONDS.toNanos(100), 0);

         assertEquals(0, earlier.compareTo(earlier));
         assertEquals(-1, earlier.compareTo(later));
         assertEquals(1, later.compareTo(earlier));
         assertThrows(IllegalArgumentException.class, () -> earlier.compareTo(delayed()));

         assertTrue(earlier.cancel(false));
         assertTrue(later.cancel(false));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void virtualScheduledFutureTaskRunsPeriodicallyUntilCancelled() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final AtomicInteger count = new AtomicInteger();
         final VirtualThreadScheduledFutureTask<Void> task = new VirtualThreadScheduledFutureTask<Void>(
               (VirtualThreadPool) pool, count::incrementAndGet, null, 0,
               TimeUnit.MILLISECONDS.toNanos(5));
         task.start();
         await(() -> count.get() >= 3, "periodic task executions");
         assertTrue(task.cancel(false));
         task.joinThread();
         final int countAfterStop = count.get();
         Thread.sleep(50);
         assertEquals(countAfterStop, count.get());
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
