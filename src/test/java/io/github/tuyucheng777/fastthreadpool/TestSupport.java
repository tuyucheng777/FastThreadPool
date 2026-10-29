package io.github.tuyucheng777.fastthreadpool;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public final class TestSupport {

   private TestSupport() {
   }

   /// Waits until `condition` becomes true, failing the test after 10 seconds.
   public static void await(BooleanSupplier condition, String description) throws InterruptedException {
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!condition.getAsBoolean()) {
         if (System.nanoTime() > deadline) {
            fail("Timed out waiting for " + description);
         }
         Thread.sleep(5);
      }
   }

   /// Shuts down the pool and asserts that it terminates.
   public static void shutdown(ThreadPool pool) throws InterruptedException {
      pool.shutdownNow();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "the pool did not terminate");
   }
}
