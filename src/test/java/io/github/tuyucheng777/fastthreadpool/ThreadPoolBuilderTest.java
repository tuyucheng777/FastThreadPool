package io.github.tuyucheng777.fastthreadpool;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.github.tuyucheng777.fastthreadpool.TestSupport.shutdown;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolBuilderTest {

   @Test
   void validatesMaxNumWorkers() {
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.builder(0));
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.builder(-1));
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.of(0));
   }

   @Test
   void validatesMinNumWorkers() {
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.builder(2).minNumWorkers(-1));
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.builder(2).minNumWorkers(3));
      assertThrows(IllegalArgumentException.class, () -> ThreadPool.of(3, 2));
   }

   @Test
   void validatesIdleTimeout() {
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).idleTimeout(-1, TimeUnit.MILLISECONDS));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).idleTimeout(Duration.ofMillis(-1)));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).idleTimeout(1, null));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).idleTimeout((Duration) null));
   }

   @Test
   void validatesTaskTimeout() {
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).taskTimeout(-1, TimeUnit.MILLISECONDS));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).taskTimeout(Duration.ofMillis(-1)));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).taskTimeout(1, null));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).taskTimeout((Duration) null));
   }

   @Test
   void validatesWatchdogInterval() {
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).watchdogInterval(0, TimeUnit.MILLISECONDS));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).watchdogInterval(Duration.ZERO));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).watchdogInterval(Duration.ofMillis(-1)));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).watchdogInterval(1, null));
      assertThrows(NullPointerException.class,
            () -> ThreadPool.builder(1).watchdogInterval((Duration) null));
   }

   @Test
   void validatesScheduledTaskWorkers() {
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).scheduledTaskWorkers(0));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).scheduledTaskWorkers(1, 2));
      assertThrows(IllegalArgumentException.class,
            () -> ThreadPool.builder(1).scheduledTaskWorkers(1, -1));
   }

   @Test
   void rejectsNullArguments() {
      final ThreadPoolBuilder builder = ThreadPool.builder(1);
      assertThrows(NullPointerException.class, () -> builder.taskQueue(null));
      assertThrows(NullPointerException.class, () -> builder.submissionHandler(null));
      assertThrows(NullPointerException.class, () -> builder.exceptionHandler(null));
      assertThrows(NullPointerException.class,
            () -> builder.metrics((io.micrometer.core.instrument.MeterRegistry) null, "prefix", List.of()));
      assertThrows(NullPointerException.class,
            () -> builder.metrics(new SimpleMeterRegistry(), null, List.of()));
      assertThrows(NullPointerException.class,
            () -> builder.metrics(new SimpleMeterRegistry(), "prefix", (Iterable<Tag>) null));
      assertThrows(NullPointerException.class,
            () -> builder.metrics(new SimpleMeterRegistry(), "prefix", (Tag[]) null));
      assertThrows(NullPointerException.class, () -> builder.metrics(null, List.of()));
   }

   @Test
   void buildsAFullyConfiguredPool() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(2)
            .minNumWorkers(1)
            .idleTimeout(Duration.ofSeconds(1))
            .idleTimeout(1, TimeUnit.SECONDS)
            .taskTimeout(Duration.ofSeconds(5))
            .taskTimeout(5, TimeUnit.SECONDS)
            .watchdogInterval(Duration.ofMillis(100))
            .watchdogInterval(100, TimeUnit.MILLISECONDS)
            .scheduledTaskWorkers(1)
            .taskQueue(() -> new ArrayBlockingQueue<>(16))
            .metrics(registry, "configured.pool", Tag.of("env", "test"))
            .metrics(registry, "configured.pool.tags", List.of(Tag.of("env", "test")))
            .build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertNotNull(pool.metrics());
         assertFalse(pool.isVirtual());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void builderMetricsWithoutTags() throws Exception {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final ThreadPool pool = ThreadPool.builder(1).metrics(registry, "no.tags").build();
      try {
         final CountDownLatch latch = new CountDownLatch(1);
         pool.execute(latch::countDown);
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         assertNotNull(registry.find("no.tags.workers").gauge());
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void buildsVirtualPool() throws Exception {
      final ThreadPool pool = ThreadPool.builderForVirtual()
            .taskTimeout(Duration.ofSeconds(5))
            .watchdogInterval(Duration.ofMillis(100))
            .build();
      try {
         assertTrue(pool.isVirtual());
         assertEquals("virtual", pool.submit(() -> "virtual").get(10, TimeUnit.SECONDS));
      } finally {
         shutdown(pool);
      }
   }

   @Test
   void poolFactories() throws Exception {
      final ThreadPool single = ThreadPool.of(1);
      try {
         assertEquals(1, single.submit(() -> 1).get(10, TimeUnit.SECONDS));
      } finally {
         shutdown(single);
      }

      final ThreadPool bounded = ThreadPool.of(1, 2);
      try {
         assertEquals(2, bounded.submit(() -> 2).get(10, TimeUnit.SECONDS));
      } finally {
         shutdown(bounded);
      }
   }
}
