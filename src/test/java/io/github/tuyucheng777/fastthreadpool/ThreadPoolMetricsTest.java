package io.github.tuyucheng777.fastthreadpool;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ThreadPoolMetricsTest {

   @Test
   void exposesAllMeters() {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final Gauge gauge = Gauge.builder("test.gauge", () -> 1).register(registry);
      final Timer timer = registry.timer("test.timer");

      final ThreadPoolMetrics metrics = new ThreadPoolMetrics(
            gauge, gauge, gauge, gauge, gauge,
            gauge, gauge, gauge, gauge,
            gauge, gauge, timer, timer, timer, gauge);

      assertSame(gauge, metrics.state());
      assertSame(gauge, metrics.workers());
      assertSame(gauge, metrics.scheduledTaskWorkers());
      assertSame(gauge, metrics.busyWorkers());
      assertSame(gauge, metrics.busyScheduledTaskWorkers());
      assertSame(gauge, metrics.minWorkers());
      assertSame(gauge, metrics.minScheduledTaskWorkers());
      assertSame(gauge, metrics.maxWorkers());
      assertSame(gauge, metrics.maxScheduledTaskWorkers());
      assertSame(gauge, metrics.taskQueueSize());
      assertSame(gauge, metrics.scheduledTaskQueueSize());
      assertSame(timer, metrics.successfulTaskExecutionTimer());
      assertSame(timer, metrics.failedTaskExecutionTimer());
      assertSame(timer, metrics.taskWaitTimer());
      assertSame(gauge, metrics.scheduledTasks());
   }

   @Test
   void optionalMetersCanBeNull() {
      final SimpleMeterRegistry registry = new SimpleMeterRegistry();
      final Gauge gauge = Gauge.builder("test.gauge", () -> 1).register(registry);
      final Timer timer = registry.timer("test.timer");

      final ThreadPoolMetrics metrics = new ThreadPoolMetrics(
            gauge, gauge, null, gauge, null,
            gauge, null, gauge, null,
            gauge, null, timer, timer, timer, gauge);

      assertNull(metrics.scheduledTaskWorkers());
      assertNull(metrics.busyScheduledTaskWorkers());
      assertNull(metrics.minScheduledTaskWorkers());
      assertNull(metrics.maxScheduledTaskWorkers());
      assertNull(metrics.scheduledTaskQueueSize());
   }
}
