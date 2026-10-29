package io.github.tuyucheng777.fastthreadpool.virtual;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import io.github.tuyucheng777.fastthreadpool.handler.TaskAction;
import io.github.tuyucheng777.fastthreadpool.handler.TaskSubmissionHandler;
import io.github.tuyucheng777.fastthreadpool.ThreadPool;
import io.github.tuyucheng777.fastthreadpool.CompletableScheduledFuture;
import io.github.tuyucheng777.fastthreadpool.TestSupport;

import static io.github.tuyucheng777.fastthreadpool.TestSupport.shutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VirtualThreadPoolTest {

   @Test
   void virtualPoolHasNoMetrics() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         assertTrue(pool.isVirtual());
         assertNull(pool.metrics());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void executeAndSubmitTasks() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));

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
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final CompletableFuture<Object> future = pool.submit((Callable<Object>) () -> {
            throw new IllegalStateException("boom");
         });
         final ExecutionException cause = assertThrows(ExecutionException.class,
               () -> future.get(10, TimeUnit.SECONDS));
         assertInstanceOf(IllegalStateException.class, cause.getCause());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void scheduledTasksRunAfterTheirDelay() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         assertEquals("scheduled", pool.schedule(() -> "scheduled", 20, TimeUnit.MILLISECONDS)
               .get(10, TimeUnit.SECONDS));

         final CountDownLatch latch = new CountDownLatch(1);
         final CompletableScheduledFuture<?> future = pool.schedule(latch::countDown, 20, TimeUnit.MILLISECONDS);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         future.get(10, TimeUnit.SECONDS);
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void periodicTasksRepeatUntilCancelled() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
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
      final ThreadPool pool = ThreadPool.ofVirtual();
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
      final ThreadPool pool = ThreadPool.ofVirtual();
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
   void shutdownNowInterruptsRunningTaskAndReturnsUnprocessedTasks() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
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

         final List<Runnable> unprocessed = pool.shutdownNow();
         // The task has already started, so it is not reported as unprocessed.
         assertTrue(unprocessed.isEmpty());
         assertTrue(interrupted.await(10, TimeUnit.SECONDS));
         assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void lateSubmissionsAreRejectedByDefault() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
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
      final ThreadPool pool = ThreadPool.builderForVirtual()
            .submissionHandler(new TaskSubmissionHandler() {
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
            })
            .build();
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
   void submissionHandlerCanRejectTasks() throws Exception {
      final ThreadPool pool = ThreadPool.builderForVirtual()
            .submissionHandler(new TaskSubmissionHandler() {
               @Override
               public TaskAction handleSubmission(Runnable task, ThreadPool threadPool) {
                  return TaskAction.reject();
               }

               @Override
               public TaskAction handleSubmission(Callable<?> task, ThreadPool threadPool) {
                  return TaskAction.reject();
               }

               @Override
               public TaskAction handleLateSubmission(Runnable task, ThreadPool threadPool) {
                  return TaskAction.reject();
               }

               @Override
               public TaskAction handleLateSubmission(Callable<?> task, ThreadPool threadPool) {
                  return TaskAction.reject();
               }
            })
            .build();
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
   void shutdownTerminatesImmediatelyWhenIdle() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      assertFalse(pool.isShutdown());
      pool.shutdown();
      assertTrue(pool.isShutdown());
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.isTerminated());
      assertTrue(pool.terminationFuture().isDone());
      pool.awaitTermination();
      pool.shutdown();
      assertTrue(pool.isTerminated());
   }

   @Test
   void awaitTerminationWithTimeout() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         assertFalse(pool.awaitTermination(10, TimeUnit.MILLISECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void watchdogInterruptsLongRunningVirtualTask() throws Exception {
      final ThreadPool pool = ThreadPool.builderForVirtual()
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
   void virtualPeriodicTasksStopAfterCancellation() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         final AtomicInteger count = new AtomicInteger();
         final CompletableScheduledFuture<?> future =
               pool.scheduleAtFixedRate(count::incrementAndGet, 0, 5, TimeUnit.MILLISECONDS);
         TestSupport.await(() -> count.get() >= 3, "periodic executions");
         future.cancel(true);
         Thread.sleep(50);
         final int countAfterCancellation = count.get();
         Thread.sleep(50);
         assertEquals(countAfterCancellation, count.get());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void invalidSubmissionsAreRejected() throws Exception {
      final ThreadPool pool = ThreadPool.ofVirtual();
      try {
         assertThrows(NullPointerException.class, () -> pool.execute(null));
         assertThrows(NullPointerException.class, () -> pool.schedule((Runnable) null, 0, TimeUnit.MILLISECONDS));
         assertThrows(NullPointerException.class,
               () -> pool.schedule((Callable<Object>) null, 0, TimeUnit.MILLISECONDS));
         assertThrows(NullPointerException.class, () -> pool.schedule(() -> {
         }, 0, null));
         assertThrows(IllegalArgumentException.class, () -> pool.schedule(() -> {
         }, -1, TimeUnit.MILLISECONDS));
         assertThrows(IllegalArgumentException.class, () -> pool.schedule(() -> {
         }, Long.MAX_VALUE, TimeUnit.NANOSECONDS));
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
}
