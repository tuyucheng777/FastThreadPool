package io.github.tuyucheng777.fastthreadpool.internal;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedThreadTest {

   @Test
   void startRunsTheTaskAndNotifiesLifecycleHooks() throws Exception {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      assertEquals(ManagedThread.UNSPECIFIED, thread.lastActivityTimeNanos());
      assertFalse(thread.isStarted());

      thread.start();
      assertEquals("result", thread.get(10, TimeUnit.SECONDS));
      thread.joinThread();
      assertTrue(thread.isStarted());
      assertEquals(1, thread.startCount.get());
      assertEquals(1, thread.stopCount.get());
      assertNotNull(thread.threadName());

      // Starting an already started thread is a no-op.
      thread.start();
      assertEquals(1, thread.startCount.get());
   }

   @Test
   void failedTaskCompletesExceptionally() throws Exception {
      final TestManagedThread thread = new TestManagedThread(() -> {
         throw new IllegalStateException("boom");
      });
      thread.start();
      final ExecutionException cause = assertThrows(ExecutionException.class,
            () -> thread.get(10, TimeUnit.SECONDS));
      assertInstanceOf(IllegalStateException.class, cause.getCause());
   }

   @Test
   void runIsNotSupported() {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      assertThrows(UnsupportedOperationException.class, thread::run);
   }

   @Test
   void completionMethodsAreNotSupported() {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      assertThrows(UnsupportedOperationException.class, () -> thread.complete("value"));
      assertThrows(UnsupportedOperationException.class,
            () -> thread.completeExceptionally(new IllegalStateException()));
      assertThrows(UnsupportedOperationException.class, () -> thread.obtrudeValue("value"));
      assertThrows(UnsupportedOperationException.class,
            () -> thread.obtrudeException(new IllegalStateException()));
      assertThrows(UnsupportedOperationException.class, () -> thread.compareTo(delayed()));
   }

   @Test
   void scheduledFutureDefaults() {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      assertFalse(thread.isPeriodic());
      assertEquals(-1, thread.getDelay(TimeUnit.NANOSECONDS));
   }

   @Test
   void activityTimestampsCanBeUpdatedAndCleared() {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      final long timestamp = thread.updateLastActivityTimeNanos();
      assertTrue(timestamp > 0);
      assertEquals(timestamp, thread.lastActivityTimeNanos());
      thread.clearLastActivityTimeNanos();
      assertEquals(ManagedThread.UNSPECIFIED, thread.lastActivityTimeNanos());
   }

   @Test
   void setProcessedCanOnlySucceedOnce() throws Exception {
      final CountDownLatch release = new CountDownLatch(1);
      final TestManagedThread thread = new TestManagedThread(() -> {
         release.await();
         return "result";
      });
      thread.start();
      assertTrue(thread.setProcessed());
      assertFalse(thread.setProcessed());
      release.countDown();
      assertEquals("result", thread.get(10, TimeUnit.SECONDS));
   }

   @Test
   void cancelInterruptsARunningTask() throws Exception {
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch interrupted = new CountDownLatch(1);
      final TestManagedThread thread = new TestManagedThread(() -> {
         started.countDown();
         try {
            Thread.sleep(30_000);
         } catch (InterruptedException e) {
            interrupted.countDown();
         }
         return "interrupted";
      });
      thread.start();
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertTrue(thread.cancel(true));
      assertTrue(interrupted.await(10, TimeUnit.SECONDS));
      thread.joinThread();
      assertTrue(thread.isCancelled());
   }

   @Test
   void cancelByShutdownNowCollectsUnprocessedTasks() throws Exception {
      final CountDownLatch started = new CountDownLatch(1);
      final TestManagedThread thread = new TestManagedThread(() -> {
         started.countDown();
         Thread.sleep(30_000);
         return "done";
      });
      thread.start();
      assertTrue(started.await(10, TimeUnit.SECONDS));

      final List<Runnable> unprocessedTasks = new ArrayList<>();
      assertTrue(thread.cancelByShutdownNow(unprocessedTasks));
      assertEquals(1, unprocessedTasks.size());
      assertSame(thread, unprocessedTasks.get(0));
      thread.joinThread();
      assertTrue(thread.isCancelled());
   }

   @Test
   void cancelByShutdownNowOnAnAlreadyCancelledThreadAddsNothing() {
      final TestManagedThread thread = new TestManagedThread(() -> "result");
      assertTrue(thread.cancel(false));
      final List<Runnable> unprocessedTasks = new ArrayList<>();
      assertTrue(thread.cancelByShutdownNow(unprocessedTasks));
      assertTrue(unprocessedTasks.isEmpty());
   }

   @Test
   void cancelledTaskDoesNotRunAfterStart() throws Exception {
      final TestManagedThread thread = new TestManagedThread(() -> {
         throw new AssertionError("should not run");
      });
      assertTrue(thread.cancel(false));
      thread.start();
      thread.joinThread();
      assertEquals(1, thread.startCount.get());
      assertEquals(1, thread.stopCount.get());
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

   private static final class TestManagedThread extends ManagedThread<String> {

      private final Callable<String> task;
      private final AtomicInteger startCount = new AtomicInteger();
      private final AtomicInteger stopCount = new AtomicInteger();

      TestManagedThread(Callable<String> task) {
         super(Executors.defaultThreadFactory());
         this.task = task;
      }

      @Override
      protected String call() throws Exception {
         return task.call();
      }

      @Override
      protected void onStart() {
         startCount.incrementAndGet();
      }

      @Override
      protected void onStop() {
         stopCount.incrementAndGet();
      }
   }
}
