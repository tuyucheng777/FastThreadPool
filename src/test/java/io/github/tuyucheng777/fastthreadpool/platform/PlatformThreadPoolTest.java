package io.github.tuyucheng777.fastthreadpool.platform;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import io.github.tuyucheng777.fastthreadpool.internal.ThreadPoolState;
import io.github.tuyucheng777.fastthreadpool.handler.TaskAction;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.handler.TaskExceptionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.ThreadPoolMetrics;
import io.github.tuyucheng777.fastthreadpool.CompletableScheduledFuture;

import static io.github.tuyucheng777.fastthreadpool.TestSupport.await;
import static io.github.tuyucheng777.fastthreadpool.TestSupport.shutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformThreadPoolTest {

   @Test
   void executeRunsTask() throws Exception {
      final ThreadPool pool = ThreadPool.of(2);
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void submitReturnsValues() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertEquals("callable", pool.submit(() -> "callable").get(10, TimeUnit.SECONDS));
         assertEquals("result", pool.submit(() -> {
         }, "result").get(10, TimeUnit.SECONDS));
         assertNull(pool.submit(() -> {
         }).get(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void failedTasksCompleteFuturesExceptionally() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final CompletableFuture<Object> future = pool.submit((Callable<Object>) () -> {
            throw new IllegalStateException("boom");
         });
         final ExecutionException cause = assertThrows(ExecutionException.class,
               () -> future.get(10, TimeUnit.SECONDS));
         assertInstanceOf(IllegalStateException.class, cause.getCause());
         assertEquals("boom", cause.getCause().getMessage());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void exceptionHandlerReceivesFailedTask() throws Exception {
      final RecordingExceptionHandler handler = new RecordingExceptionHandler();
      final ThreadPool pool = ThreadPool.builder(1).exceptionHandler(handler).build();
      try {
         pool.execute(() -> {
            throw new IllegalArgumentException("failed runnable");
         });
         assertTrue(handler.latch.await(10, TimeUnit.SECONDS));
         assertEquals("failed runnable", handler.failure.get().getMessage());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void customSubmissionHandlerRejectsTasks() throws Exception {
      final RejectingSubmissionHandler handler = new RejectingSubmissionHandler();
      final ThreadPool pool = ThreadPool.builder(1).submissionHandler(handler).build();
      try {
         assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {
         }));
         assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {
         }));

         handler.reject.set(false);
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void lateSubmissionsAreRejectedByDefault() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      pool.shutdown();
      try {
         assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {
         }));
         assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> {
         }));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void lateSubmissionsCanBeLogged() throws Exception {
      final RejectingSubmissionHandler handler = new RejectingSubmissionHandler();
      handler.logLateSubmissions.set(true);
      final ThreadPool pool = ThreadPool.builder(1).submissionHandler(handler).build();
      pool.shutdown();
      try {
         pool.execute(() -> {
         });
         pool.submit(() -> {
         });
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void shutdownTerminatesThePool() throws Exception {
      final ThreadPool pool = ThreadPool.builder(2).minNumWorkers(1).build();
      final CountDownLatch latch = new CountDownLatch(1);
      pool.execute(latch::countDown);
      assertTrue(latch.await(10, TimeUnit.SECONDS));

      assertFalse(pool.isShutdown());
      assertNull(pool.metrics());

      pool.shutdown();
      assertTrue(pool.isShutdown());
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.isTerminated());
      assertTrue(pool.terminationFuture().isDone());
      pool.awaitTermination();

      // Shutdown is idempotent.
      pool.shutdown();
      assertTrue(pool.isTerminated());
   }

   @Test
   void shutdownWithoutWorkersTerminatesImmediately() throws Exception {
      final ThreadPool pool = ThreadPool.of(2);
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.isTerminated());
      shutdown(pool);
   }

   @Test
   void shutdownNowReturnsQueuedTasksAndInterruptsRunningTask() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch interrupted = new CountDownLatch(1);
      try {
         pool.execute(() -> {
            started.countDown();
            try {
               Thread.sleep(30_000);
            } catch (InterruptedException e) {
               interrupted.countDown();
            }
         });
         assertTrue(started.await(10, TimeUnit.SECONDS));

         final AtomicInteger executed = new AtomicInteger();
         for (int i = 0; i < 3; i++) {
            pool.execute(executed::incrementAndGet);
         }

         final List<Runnable> unprocessed = pool.shutdownNow();
         assertEquals(3, unprocessed.size());
         assertTrue(interrupted.await(10, TimeUnit.SECONDS));
         assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
         assertEquals(0, executed.get());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void shutdownNowEscalatesFromShutdown() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch interrupted = new CountDownLatch(1);
      try {
         pool.execute(() -> {
            started.countDown();
            try {
               Thread.sleep(30_000);
            } catch (InterruptedException e) {
               interrupted.countDown();
            }
         });
         assertTrue(started.await(10, TimeUnit.SECONDS));

         pool.shutdown();
         pool.shutdownNow();
         assertTrue(interrupted.await(10, TimeUnit.SECONDS));
         assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void awaitTerminationWithTimeout() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertFalse(pool.awaitTermination(10, TimeUnit.MILLISECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void idleWorkersExpire() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(2)
            .idleTimeout(Duration.ofMillis(50))
            .metrics(registry, "idle.pool")
            .build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         await(() -> pool.metrics().workers().value() == 0, "idle workers to expire");
         assertEquals(0, pool.metrics().busyWorkers().value());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void dedicatedScheduledTaskWorkers() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(1)
            .scheduledTaskWorkers(2, 1)
            .metrics(registry, "scheduled.pool")
            .build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.schedule(latch::countDown, 10, TimeUnit.MILLISECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));

         final ThreadPoolMetrics metrics = pool.metrics();
         assertNotNull(metrics.scheduledTaskWorkers());
         assertNotNull(metrics.busyScheduledTaskWorkers());
         assertNotNull(metrics.minScheduledTaskWorkers());
         assertNotNull(metrics.maxScheduledTaskWorkers());
         assertNotNull(metrics.scheduledTaskQueueSize());
         await(() -> metrics.scheduledTasks().value() == 0, "the scheduled task count to return to zero");
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void metricsReportTaskResultsAndWaits() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(2)
            .minNumWorkers(1)
            .metrics(registry, "test.pool", Tag.of("foo", "bar"))
            .build();
      try {
         assertEquals("ok", pool.submit(() -> "ok").get(10, TimeUnit.SECONDS));
         final CompletableFuture<Object> failed = pool.submit((Callable<Object>) () -> {
            throw new IllegalStateException("nope");
         });
         assertThrows(ExecutionException.class, () -> failed.get(10, TimeUnit.SECONDS));

         final CompletableScheduledFuture<?> scheduled =
               pool.schedule(() -> {
               }, 10, TimeUnit.MILLISECONDS);
         scheduled.get(10, TimeUnit.SECONDS);

         final ThreadPoolMetrics metrics = pool.metrics();
         assertNotNull(metrics);
         assertNotNull(registry.find("test.pool.state").gauge());
         assertNotNull(registry.find("test.pool.workers").gauge());
         assertNotNull(registry.find("test.pool.busy.workers").gauge());
         assertNotNull(registry.find("test.pool.min.workers").gauge());
         assertNotNull(registry.find("test.pool.max.workers").gauge());
         assertNotNull(registry.find("test.pool.task.queue.size").gauge());
         assertNotNull(registry.find("test.pool.scheduled.tasks").gauge());
         assertNotNull(registry.find("test.pool.task.wait").timer());
         assertNull(metrics.scheduledTaskWorkers());
         assertNull(metrics.scheduledTaskQueueSize());

         final Timer successTimer = metrics.successfulTaskExecutionTimer();
         final Timer failureTimer = metrics.failedTaskExecutionTimer();
         await(() -> successTimer.count() >= 1, "a successful task execution");
         await(() -> failureTimer.count() >= 1, "a failed task execution");
         assertTrue(metrics.taskWaitTimer().count() >= 2);
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void metricsCanUseTheGlobalRegistry() throws Exception {
      final ThreadPool pool = ThreadPool.builder(1)
            .metrics("global.pool", List.of(Tag.of("scope", "test")))
            .build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertNotNull(pool.metrics());
         assertNotNull(Metrics.globalRegistry.find("global.pool.state").gauge());
      } finally {
         shutdown(pool);
         Metrics.globalRegistry.clear();
      }
   }

   @Test
   void watchdogInterruptsLongRunningTask() throws Exception {
      final ThreadPool pool = ThreadPool.builder(1)
            .taskTimeout(Duration.ofMillis(50))
            .watchdogInterval(Duration.ofMillis(10))
            .build();
      final CountDownLatch started = new CountDownLatch(1);
      final CountDownLatch interrupted = new CountDownLatch(1);
      try {
         pool.execute(() -> {
            started.countDown();
            try {
               Thread.sleep(30_000);
            } catch (InterruptedException e) {
               interrupted.countDown();
            }
         });
         assertTrue(started.await(10, TimeUnit.SECONDS));
         assertTrue(interrupted.await(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void taskQueueFactoryIsUsed() throws Exception {
      final AtomicInteger created = new AtomicInteger();
      final ThreadPool pool = ThreadPool.builder(2)
            .taskQueue(() -> {
               created.incrementAndGet();
               return new LinkedBlockingQueue<>();
            })
            .scheduledTaskWorkers(1)
            .build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertEquals(2, created.get());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void scheduledTasksReturnValues() throws Exception {
      final ThreadPool pool = ThreadPool.of(2);
      try {
         assertEquals("scheduled", pool.schedule(() -> "scheduled", 10, TimeUnit.MILLISECONDS)
               .get(10, TimeUnit.SECONDS));

         final CountDownLatch latch = new CountDownLatch(1);
         final CompletableScheduledFuture<?> future = pool.schedule(latch::countDown, 10, TimeUnit.MILLISECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         future.get(10, TimeUnit.SECONDS);
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void periodicTasksRepeatUntilCancelled() throws Exception {
      final ThreadPool pool = ThreadPool.of(2);
      try {
         final CountDownLatch fixedRate = new CountDownLatch(3);
         final CompletableScheduledFuture<?> rateFuture =
               pool.scheduleAtFixedRate(fixedRate::countDown, 0, 10, TimeUnit.MILLISECONDS);
         assertTrue(fixedRate.await(10, TimeUnit.SECONDS));
         assertTrue(rateFuture.cancel(false));

         final CountDownLatch fixedDelay = new CountDownLatch(3);
         final CompletableScheduledFuture<?> delayFuture =
               pool.scheduleWithFixedDelay(fixedDelay::countDown, 0, 10, TimeUnit.MILLISECONDS);
         assertTrue(fixedDelay.await(10, TimeUnit.SECONDS));
         assertTrue(delayFuture.cancel(false));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void periodicTaskFailureCompletesTheFuture() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final CompletableScheduledFuture<?> future = pool.scheduleAtFixedRate(() -> {
            throw new IllegalStateException("periodic failure");
         }, 0, 10, TimeUnit.MILLISECONDS);

         final ExecutionException cause = assertThrows(ExecutionException.class,
               () -> future.get(10, TimeUnit.SECONDS));
         assertInstanceOf(IllegalStateException.class, cause.getCause());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void cancellingScheduledTaskPreventsExecution() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final AtomicBoolean ran = new AtomicBoolean();
         final CompletableScheduledFuture<?> future = pool.schedule(() -> ran.set(true), 2, TimeUnit.SECONDS);
         assertTrue(future.cancel(false));
         assertTrue(future.isCancelled());
         Thread.sleep(50);
         assertFalse(ran.get());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void manyScheduledTasksAreKeptInOrder() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         final List<CompletableScheduledFuture<?>> futures = new ArrayList<>();
         for (int i = 0; i < 1100; i++) {
            futures.add(pool.schedule(() -> {
            }, 5, TimeUnit.SECONDS));
         }
         // Cancel from the middle outwards to exercise the min-heap removal logic.
         for (int i = futures.size() / 2; i < futures.size(); i++) {
            assertTrue(futures.get(i).cancel(false));
         }
         for (int i = futures.size() / 2 - 1; i >= 0; i--) {
            assertTrue(futures.get(i).cancel(false));
         }
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void scheduledTaskWorkersRunPeriodicTasksDuringShutdown() throws Exception {
      final ThreadPool pool = ThreadPool.builder(1).scheduledTaskWorkers(1).build();
      try {
         final AtomicInteger count = new AtomicInteger();
         pool.scheduleAtFixedRate(count::incrementAndGet, 0, 1, TimeUnit.MILLISECONDS);
         await(() -> count.get() >= 3, "periodic task executions");
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void invalidSubmissionsAreRejected() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertThrows(NullPointerException.class, () -> pool.execute(null));
         assertThrows(NullPointerException.class, () -> pool.submit((Runnable) null));
         assertThrows(NullPointerException.class, () -> pool.submit((Callable<Object>) null));
         assertThrows(NullPointerException.class, () -> pool.schedule((Runnable) null, 0, TimeUnit.MILLISECONDS));
         assertThrows(NullPointerException.class,
               () -> pool.schedule((Callable<Object>) null, 0, TimeUnit.MILLISECONDS));
         assertThrows(NullPointerException.class, () -> pool.schedule(() -> {
         }, 0, null));

         assertThrows(IllegalArgumentException.class, () -> pool.schedule(() -> {
         }, -1, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.schedule(() -> {
         }, Long.MAX_VALUE, TimeUnit.NANOSECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.schedule(() -> {
         }, -1, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.scheduleAtFixedRate(() -> {
         }, 0, -1, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.scheduleAtFixedRate(() -> {
         }, 0, 0, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.scheduleWithFixedDelay(() -> {
         }, 0, -1, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.scheduleWithFixedDelay(() -> {
         }, 0, 0, TimeUnit.MILLISECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void isVirtualIsFalse() throws Exception {
      final ThreadPool pool = ThreadPool.of(1);
      try {
         assertFalse(pool.isVirtual());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void platformStateIsExposedToMetrics() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(1).metrics(registry, "state.pool").build();
      try {
         assertEquals(ThreadPoolState.STARTED.ordinal(), (int) pool.metrics().state().value());
      } finally {
         shutdown(pool);
      }
   }

   private static final class RecordingExceptionHandler implements TaskExceptionHandler {

      private final AtomicReference<Throwable> failure = new AtomicReference<>();
      private final CountDownLatch latch = new CountDownLatch(1);

      @Override
      public void handleTaskException(Runnable task, Throwable cause, ThreadPool threadPool) {
         failure.set(cause);
         latch.countDown();
      }

      @Override
      public void handleTaskException(Callable<?> task, Throwable cause, ThreadPool threadPool) {
         failure.set(cause);
         latch.countDown();
      }
   }

   private static final class RejectingSubmissionHandler implements TaskSubmissionHandler {

      private final AtomicBoolean reject = new AtomicBoolean(true);
      private final AtomicBoolean logLateSubmissions = new AtomicBoolean();

      @Override
      public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
         return reject.get() ? TaskAction.reject() : TaskAction.accept();
      }

      @Override
      public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
         return reject.get() ? TaskAction.reject() : TaskAction.accept();
      }

      @Override
      public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
         return logLateSubmissions.get() ? TaskAction.log() : TaskAction.reject();
      }

      @Override
      public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
         return logLateSubmissions.get() ? TaskAction.log() : TaskAction.reject();
      }
   }
}
