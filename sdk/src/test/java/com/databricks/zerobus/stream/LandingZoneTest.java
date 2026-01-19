package com.databricks.zerobus.stream;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Comprehensive tests for LandingZone thread safety and functionality. */
class LandingZoneTest {

  // Helper to create a test batch
  private InflightBatch<String> createBatch(long offsetId, String... records) {
    List<String> recordList = records.length > 0 ? Arrays.asList(records) : null;
    EncodedBatch encoded = EncodedBatch.jsonSingle("test");
    return new InflightBatch<>(recordList, encoded, offsetId, new CompletableFuture<>());
  }

  // ==================== Basic Operations ====================

  @Test
  void testAddAndObserve() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    InflightBatch<String> batch = createBatch(1, "record1");
    zone.add(batch);

    assertEquals(1, zone.pendingCount());
    assertEquals(0, zone.inflightCount());
    assertEquals(1, zone.totalCount());

    InflightBatch<String> observed = zone.observe();
    assertSame(batch, observed);

    assertEquals(0, zone.pendingCount());
    assertEquals(1, zone.inflightCount());
    assertEquals(1, zone.totalCount());
  }

  @Test
  void testRemoveObserved() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1, "record1"));
    zone.observe();

    assertEquals(9, zone.availablePermits());

    InflightBatch<String> removed = zone.removeObserved();
    assertNotNull(removed);
    assertEquals(1, removed.offsetId);

    assertEquals(10, zone.availablePermits());
    assertEquals(0, zone.inflightCount());
  }

  @Test
  void testRemoveObservedEmpty() {
    LandingZone<String> zone = new LandingZone<>(10);
    assertNull(zone.removeObserved());
  }

  @Test
  void testMultipleBatches() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1, "a"));
    zone.add(createBatch(2, "b"));
    zone.add(createBatch(3, "c"));

    assertEquals(3, zone.pendingCount());

    // Observe in order
    assertEquals(1, zone.observe().offsetId);
    assertEquals(2, zone.observe().offsetId);
    assertEquals(3, zone.observe().offsetId);

    assertEquals(0, zone.pendingCount());
    assertEquals(3, zone.inflightCount());

    // Remove in order
    assertEquals(1, zone.removeObserved().offsetId);
    assertEquals(2, zone.removeObserved().offsetId);
    assertEquals(3, zone.removeObserved().offsetId);

    assertEquals(0, zone.totalCount());
  }

  // ==================== Backpressure ====================

  @Test
  @Timeout(5)
  void testBackpressureBlocks() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(2);

    zone.add(createBatch(1));
    zone.add(createBatch(2));

    assertEquals(0, zone.availablePermits());

    // Third add should block
    AtomicBoolean added = new AtomicBoolean(false);
    Thread adder =
        new Thread(
            () -> {
              try {
                zone.add(createBatch(3));
                added.set(true);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    adder.start();

    Thread.sleep(100);
    assertFalse(added.get(), "Add should be blocked");

    // Observe and remove to release permit
    zone.observe();
    zone.removeObserved();

    adder.join(1000);
    assertTrue(added.get(), "Add should complete after permit released");
  }

  @Test
  void testTryAddTimeout() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(1);

    zone.add(createBatch(1));

    // Should timeout since capacity is full
    long start = System.currentTimeMillis();
    boolean result = zone.tryAdd(createBatch(2), 100, TimeUnit.MILLISECONDS);
    long elapsed = System.currentTimeMillis() - start;

    assertFalse(result);
    assertTrue(elapsed >= 90, "Should wait approximately 100ms");
  }

  @Test
  void testTryAddSuccess() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(2);

    assertTrue(zone.tryAdd(createBatch(1), 100, TimeUnit.MILLISECONDS));
    assertEquals(1, zone.pendingCount());
  }

  // ==================== Observe Blocking ====================

  @Test
  @Timeout(5)
  void testObserveBlocks() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    AtomicReference<InflightBatch<String>> observed = new AtomicReference<>();
    Thread observer =
        new Thread(
            () -> {
              try {
                observed.set(zone.observe());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    observer.start();

    Thread.sleep(100);
    assertNull(observed.get(), "Observe should be blocked");

    // Add a batch
    zone.add(createBatch(42));

    observer.join(1000);
    assertNotNull(observed.get());
    assertEquals(42, observed.get().offsetId);
  }

  @Test
  void testTryObserveTimeout() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    long start = System.currentTimeMillis();
    InflightBatch<String> result = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    long elapsed = System.currentTimeMillis() - start;

    assertNull(result);
    assertTrue(elapsed >= 90, "Should wait approximately 100ms");
  }

  @Test
  void testTryObserveSuccess() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1));

    InflightBatch<String> result = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    assertNotNull(result);
    assertEquals(1, result.offsetId);
  }

  // ==================== Recovery ====================

  @Test
  void testResetObserve() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1, "a"));
    zone.add(createBatch(2, "b"));
    zone.add(createBatch(3, "c"));

    // Observe first two
    zone.observe();
    zone.observe();

    assertEquals(1, zone.pendingCount());
    assertEquals(2, zone.inflightCount());

    // Reset moves inflight back to pending
    zone.resetObserve();

    assertEquals(3, zone.pendingCount());
    assertEquals(0, zone.inflightCount());

    // Order should be preserved: 1, 2, 3
    assertEquals(1, zone.observe().offsetId);
    assertEquals(2, zone.observe().offsetId);
    assertEquals(3, zone.observe().offsetId);
  }

  @Test
  void testResetObserveEmpty() {
    LandingZone<String> zone = new LandingZone<>(10);
    zone.resetObserve(); // Should not throw
    assertEquals(0, zone.totalCount());
  }

  // ==================== Failure Handling ====================

  @Test
  void testRemoveAll() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1, "a"));
    zone.add(createBatch(2, "b"));
    zone.observe(); // Move 1 to inflight
    zone.add(createBatch(3, "c"));

    assertEquals(2, zone.pendingCount()); // 2, 3
    assertEquals(1, zone.inflightCount()); // 1

    List<InflightBatch<String>> all = zone.removeAll();

    assertEquals(3, all.size());
    // Inflight first, then pending
    assertEquals(1, all.get(0).offsetId);
    assertEquals(2, all.get(1).offsetId);
    assertEquals(3, all.get(2).offsetId);

    assertEquals(0, zone.totalCount());
    assertEquals(10, zone.availablePermits());
  }

  @Test
  void testRemoveAllEmpty() {
    LandingZone<String> zone = new LandingZone<>(10);
    List<InflightBatch<String>> all = zone.removeAll();
    assertTrue(all.isEmpty());
  }

  // ==================== Record Access ====================

  @Test
  void testGetAllRecords() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1, "a", "b"));
    zone.add(createBatch(2, "c"));
    zone.observe(); // Move 1 to inflight
    zone.add(createBatch(3, "d", "e", "f"));

    List<String> records = zone.getAllRecords();

    // Should include all records from both queues
    assertEquals(6, records.size());
    assertTrue(records.containsAll(Arrays.asList("a", "b", "c", "d", "e", "f")));
  }

  @Test
  void testGetAllRecordsWithNullsInPending() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    // Batch with no records (null)
    zone.add(createBatch(1));
    // Batch with records
    zone.add(createBatch(2, "x", "y"));

    List<String> records = zone.getAllRecords();

    assertEquals(2, records.size());
    assertTrue(records.contains("x"));
    assertTrue(records.contains("y"));
  }

  @Test
  void testGetAllRecordsWithNullsInInflight() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    // Add batch with no records (null) and move to inflight
    zone.add(createBatch(1));
    zone.observe(); // Moves to inflight

    // Add batch with records to pending
    zone.add(createBatch(2, "a", "b"));

    List<String> records = zone.getAllRecords();

    // Should only contain records from batch 2 (pending queue)
    // Batch 1 in inflight has null records
    assertEquals(2, records.size());
    assertTrue(records.contains("a"));
    assertTrue(records.contains("b"));
  }

  @Test
  void testPeekAll() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    zone.add(createBatch(1));
    zone.add(createBatch(2));
    zone.observe(); // Move 1 to inflight

    List<InflightBatch<String>> all = zone.peekAll();

    assertEquals(2, all.size());
    assertEquals(1, all.get(0).offsetId); // Inflight first
    assertEquals(2, all.get(1).offsetId); // Then pending

    // Should not remove anything
    assertEquals(2, zone.totalCount());
  }

  // ==================== Close Behavior ====================

  @Test
  void testClose() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    assertFalse(zone.isClosed());
    zone.close();
    assertTrue(zone.isClosed());
  }

  @Test
  void testAddAfterClose() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);
    zone.close();

    assertThrows(IllegalStateException.class, () -> zone.add(createBatch(1)));
  }

  @Test
  void testTryAddAfterClose() {
    LandingZone<String> zone = new LandingZone<>(10);
    zone.close();

    assertThrows(
        IllegalStateException.class, () -> zone.tryAdd(createBatch(1), 100, TimeUnit.MILLISECONDS));
  }

  @Test
  @Timeout(5)
  void testTryAddClosedWhileWaitingForSemaphore() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(1);
    zone.add(createBatch(1)); // Fill capacity

    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread adder =
        new Thread(
            () -> {
              try {
                zone.tryAdd(createBatch(2), 5, TimeUnit.SECONDS);
              } catch (Exception e) {
                error.set(e);
              }
            });
    adder.start();

    Thread.sleep(100);
    zone.close();

    // Release the semaphore so the thread can proceed to the closed check inside lock
    zone.observe();
    zone.removeObserved();

    adder.join(1000);
    assertNotNull(error.get());
    assertTrue(error.get() instanceof IllegalStateException);
  }

  @Test
  @Timeout(5)
  void testObserveUnblocksOnClose() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    AtomicBoolean interrupted = new AtomicBoolean(false);
    Thread observer =
        new Thread(
            () -> {
              try {
                zone.observe();
              } catch (InterruptedException e) {
                interrupted.set(true);
              }
            });
    observer.start();

    Thread.sleep(100);
    zone.close();

    observer.join(1000);
    assertTrue(interrupted.get(), "Observe should throw InterruptedException on close");
  }

  @Test
  void testTryObserveReturnsNullOnClose() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);
    zone.close();

    InflightBatch<String> result = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    assertNull(result);
  }

  @Test
  @Timeout(5)
  void testTryObserveUnblocksOnClose() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    AtomicReference<InflightBatch<String>> result = new AtomicReference<>();
    AtomicBoolean completed = new AtomicBoolean(false);
    Thread observer =
        new Thread(
            () -> {
              try {
                result.set(zone.tryObserve(5, TimeUnit.SECONDS));
                completed.set(true);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    observer.start();

    Thread.sleep(100);
    assertFalse(completed.get(), "tryObserve should still be waiting");

    zone.close();

    observer.join(1000);
    assertTrue(completed.get(), "tryObserve should complete after close");
    assertNull(result.get(), "Result should be null when closed with empty queue");
  }

  @Test
  void testTryObserveClosedWithPendingBatches() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    // Add batches before closing
    zone.add(createBatch(1, "a"));
    zone.add(createBatch(2, "b"));

    // Close zone - but batches should still be retrievable
    zone.close();

    // tryObserve should still return batches even when closed
    InflightBatch<String> batch1 = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    assertNotNull(batch1, "Should get batch even when closed");
    assertEquals(1, batch1.offsetId);

    InflightBatch<String> batch2 = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    assertNotNull(batch2, "Should get second batch even when closed");
    assertEquals(2, batch2.offsetId);

    // Now queue is empty and closed
    InflightBatch<String> batch3 = zone.tryObserve(100, TimeUnit.MILLISECONDS);
    assertNull(batch3, "Should return null when closed and empty");
  }

  @Test
  @Timeout(5)
  void testCloseWhileAddBlocked() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(1);
    zone.add(createBatch(1));

    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread adder =
        new Thread(
            () -> {
              try {
                zone.add(createBatch(2));
              } catch (Exception e) {
                error.set(e);
              }
            });
    adder.start();

    Thread.sleep(100);
    zone.close();

    // Release the semaphore so the thread can proceed to the lock
    zone.observe();
    zone.removeObserved();

    adder.join(1000);
    assertNotNull(error.get());
    assertTrue(error.get() instanceof IllegalStateException);
  }

  // ==================== Concurrent Access ====================

  @Test
  @Timeout(10)
  void testConcurrentAddAndObserve() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(100);
    int numBatches = 1000;
    AtomicInteger addedCount = new AtomicInteger(0);
    AtomicInteger observedCount = new AtomicInteger(0);
    CountDownLatch done = new CountDownLatch(2);

    // Producer thread
    Thread producer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < numBatches; i++) {
                  zone.add(createBatch(i, "record" + i));
                  addedCount.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });

    // Consumer thread
    Thread consumer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < numBatches; i++) {
                  zone.observe();
                  zone.removeObserved();
                  observedCount.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });

    producer.start();
    consumer.start();

    done.await();

    assertEquals(numBatches, addedCount.get());
    assertEquals(numBatches, observedCount.get());
    assertEquals(0, zone.totalCount());
  }

  @Test
  @Timeout(10)
  void testMultipleProducers() throws InterruptedException {
    int numProducers = 4;
    int batchesPerProducer = 25;
    int totalBatches = numProducers * batchesPerProducer;
    LandingZone<String> zone = new LandingZone<>(totalBatches); // Enough capacity
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numProducers);
    AtomicInteger totalAdded = new AtomicInteger(0);

    List<Thread> producers = new ArrayList<>();
    for (int p = 0; p < numProducers; p++) {
      final int producerId = p;
      Thread producer =
          new Thread(
              () -> {
                try {
                  startLatch.await();
                  for (int i = 0; i < batchesPerProducer; i++) {
                    zone.add(createBatch(producerId * 1000 + i));
                    totalAdded.incrementAndGet();
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } finally {
                  doneLatch.countDown();
                }
              });
      producers.add(producer);
      producer.start();
    }

    startLatch.countDown();
    doneLatch.await();

    assertEquals(totalBatches, totalAdded.get());
    assertEquals(totalBatches, zone.pendingCount());
  }

  @Test
  @Timeout(10)
  void testConcurrentResetObserve() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(100);

    // Add batches
    for (int i = 0; i < 50; i++) {
      zone.add(createBatch(i));
    }

    // Observer thread
    AtomicInteger observeCount = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(true);

    Thread observer =
        new Thread(
            () -> {
              while (running.get()) {
                try {
                  InflightBatch<String> batch = zone.tryObserve(10, TimeUnit.MILLISECONDS);
                  if (batch != null) {
                    observeCount.incrementAndGet();
                  }
                } catch (InterruptedException e) {
                  break;
                }
              }
            });

    // Resetter thread
    Thread resetter =
        new Thread(
            () -> {
              for (int i = 0; i < 10; i++) {
                zone.resetObserve();
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                  break;
                }
              }
            });

    observer.start();
    resetter.start();

    resetter.join();
    running.set(false);
    observer.join();

    // No exceptions should have occurred, and counts should be consistent
    assertEquals(zone.pendingCount() + zone.inflightCount(), zone.totalCount());
  }

  // ==================== Edge Cases ====================

  @Test
  void testSingleCapacity() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(1);

    zone.add(createBatch(1));
    assertEquals(0, zone.availablePermits());

    zone.observe();
    zone.removeObserved();
    assertEquals(1, zone.availablePermits());

    zone.add(createBatch(2));
    assertEquals(0, zone.availablePermits());
  }

  @Test
  void testBatchSize() throws InterruptedException {
    LandingZone<String> zone = new LandingZone<>(10);

    // Create batch with multiple records
    List<String> records = Arrays.asList("a", "b", "c");
    EncodedBatch encoded = EncodedBatch.jsonBatch(Arrays.asList("a", "b", "c"));
    InflightBatch<String> batch =
        new InflightBatch<>(records, encoded, 1, new CompletableFuture<>());

    zone.add(batch);
    InflightBatch<String> observed = zone.observe();

    assertEquals(3, observed.size());
    assertEquals(3, observed.records.size());
  }

  @Test
  void testInflightBatchFields() {
    List<String> records = Collections.singletonList("test");
    EncodedBatch encoded = EncodedBatch.jsonSingle("test");
    CompletableFuture<Long> promise = new CompletableFuture<>();

    InflightBatch<String> batch = new InflightBatch<>(records, encoded, 42L, promise);

    assertEquals(records, batch.records);
    assertSame(encoded, batch.encodedBatch);
    assertEquals(42L, batch.offsetId);
    assertSame(promise, batch.ackPromise);
    assertEquals(1, batch.size());
  }

  @Test
  void testInflightBatchNullRecords() {
    EncodedBatch encoded = EncodedBatch.jsonSingle("test");
    InflightBatch<String> batch = new InflightBatch<>(null, encoded, 1L, new CompletableFuture<>());

    assertNull(batch.records);
    assertEquals(1, batch.size());
  }
}
