package com.databricks.zerobus.stream;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Manages batches through their lifecycle: pending → inflight → acknowledged.
 *
 * <p>This class provides thread-safe coordination between:
 *
 * <ul>
 *   <li>Producer threads calling {@link #add} to queue batches
 *   <li>Sender thread calling {@link #observe} to get batches for sending
 *   <li>Receiver thread calling {@link #removeObserved} when batches are acknowledged
 * </ul>
 *
 * <p>Backpressure is enforced via a semaphore that limits the maximum number of inflight batches.
 *
 * @param <P> The primary record type (Message for proto, Map for JSON)
 */
final class LandingZone<P> {

  private final LinkedList<InflightBatch<P>> pendingQueue = new LinkedList<>();
  private final LinkedList<InflightBatch<P>> inflightQueue = new LinkedList<>();

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final Semaphore semaphore;

  private volatile boolean closed = false;

  /**
   * Creates a new landing zone with the specified capacity.
   *
   * @param maxInflightBatches Maximum number of batches that can be pending or inflight
   */
  LandingZone(int maxInflightBatches) {
    this.semaphore = new Semaphore(maxInflightBatches);
  }

  /**
   * Adds a batch to the pending queue.
   *
   * <p>This method blocks if the maximum inflight capacity is reached, providing backpressure.
   *
   * @param batch The batch to add
   * @throws InterruptedException if interrupted while waiting for capacity
   * @throws IllegalStateException if the landing zone is closed
   */
  void add(InflightBatch<P> batch) throws InterruptedException {
    if (closed) {
      throw new IllegalStateException("LandingZone is closed");
    }

    // Acquire permit (blocks if at capacity)
    semaphore.acquire();

    lock.lock();
    try {
      if (closed) {
        semaphore.release();
        throw new IllegalStateException("LandingZone is closed");
      }
      pendingQueue.addLast(batch);
      notEmpty.signal();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Tries to add a batch to the pending queue with a timeout.
   *
   * @param batch The batch to add
   * @param timeout Maximum time to wait
   * @param unit Time unit for timeout
   * @return true if added successfully, false if timeout elapsed
   * @throws InterruptedException if interrupted while waiting
   * @throws IllegalStateException if the landing zone is closed
   */
  boolean tryAdd(InflightBatch<P> batch, long timeout, TimeUnit unit) throws InterruptedException {
    if (closed) {
      throw new IllegalStateException("LandingZone is closed");
    }

    // Try to acquire permit with timeout
    if (!semaphore.tryAcquire(timeout, unit)) {
      return false;
    }

    lock.lock();
    try {
      if (closed) {
        semaphore.release();
        throw new IllegalStateException("LandingZone is closed");
      }
      pendingQueue.addLast(batch);
      notEmpty.signal();
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gets the next pending batch and moves it to the inflight queue.
   *
   * <p>This method blocks until a batch is available.
   *
   * @return The next batch to send
   * @throws InterruptedException if interrupted while waiting
   */
  InflightBatch<P> observe() throws InterruptedException {
    lock.lock();
    try {
      while (pendingQueue.isEmpty() && !closed) {
        notEmpty.await();
      }

      if (closed && pendingQueue.isEmpty()) {
        throw new InterruptedException("LandingZone closed");
      }

      InflightBatch<P> batch = pendingQueue.removeFirst();
      inflightQueue.addLast(batch);
      return batch;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Tries to get the next pending batch with a timeout.
   *
   * @param timeout Maximum time to wait
   * @param unit Time unit for timeout
   * @return The next batch, or null if timeout elapsed or closed
   * @throws InterruptedException if interrupted while waiting
   */
  @Nullable InflightBatch<P> tryObserve(long timeout, TimeUnit unit) throws InterruptedException {
    long nanos = unit.toNanos(timeout);
    lock.lock();
    try {
      while (pendingQueue.isEmpty() && !closed) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }

      if (closed && pendingQueue.isEmpty()) {
        return null;
      }

      InflightBatch<P> batch = pendingQueue.removeFirst();
      inflightQueue.addLast(batch);
      return batch;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes the oldest batch from the inflight queue (after acknowledgment).
   *
   * <p>This releases a semaphore permit, allowing another batch to be added.
   *
   * @return The acknowledged batch, or null if the inflight queue is empty
   */
  @Nullable InflightBatch<P> removeObserved() {
    lock.lock();
    try {
      InflightBatch<P> batch = inflightQueue.pollFirst();
      if (batch != null) {
        semaphore.release();
      }
      return batch;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Resets for recovery: moves all inflight batches back to the pending queue.
   *
   * <p>This is called when a stream fails and needs to retry sending. The batches are moved in
   * reverse order so they maintain their original order when re-observed.
   */
  void resetObserve() {
    lock.lock();
    try {
      // Move inflight back to front of pending (in reverse order to maintain order)
      while (!inflightQueue.isEmpty()) {
        InflightBatch<P> batch = inflightQueue.removeLast();
        pendingQueue.addFirst(batch);
      }
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Removes and returns all batches (both pending and inflight).
   *
   * <p>This is called when the stream fails permanently. All semaphore permits are released.
   *
   * @return List of all batches that were in the landing zone
   */
  List<InflightBatch<P>> removeAll() {
    lock.lock();
    try {
      List<InflightBatch<P>> all = new ArrayList<>(pendingQueue.size() + inflightQueue.size());
      all.addAll(inflightQueue);
      all.addAll(pendingQueue);

      int count = inflightQueue.size() + pendingQueue.size();
      inflightQueue.clear();
      pendingQueue.clear();

      // Release all permits
      semaphore.release(count);

      return all;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns all records from all batches (flattened).
   *
   * <p>Records from batches where the original records were not stored (e.g., byte[] ingestion) are
   * skipped.
   *
   * @return List of all original records
   */
  List<P> getAllRecords() {
    lock.lock();
    try {
      List<P> records = new ArrayList<>();
      for (InflightBatch<P> batch : inflightQueue) {
        if (batch.records != null) {
          records.addAll(batch.records);
        }
      }
      for (InflightBatch<P> batch : pendingQueue) {
        if (batch.records != null) {
          records.addAll(batch.records);
        }
      }
      return records;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Returns all batches without removing them.
   *
   * @return List of all batches (inflight first, then pending)
   */
  List<InflightBatch<P>> peekAll() {
    lock.lock();
    try {
      List<InflightBatch<P>> all = new ArrayList<>(pendingQueue.size() + inflightQueue.size());
      all.addAll(inflightQueue);
      all.addAll(pendingQueue);
      return all;
    } finally {
      lock.unlock();
    }
  }

  /** Closes this landing zone, waking up any blocked threads. */
  void close() {
    lock.lock();
    try {
      closed = true;
      notEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** Returns whether this landing zone is closed. */
  boolean isClosed() {
    return closed;
  }

  /** Returns the number of pending batches. */
  int pendingCount() {
    lock.lock();
    try {
      return pendingQueue.size();
    } finally {
      lock.unlock();
    }
  }

  /** Returns the number of inflight batches. */
  int inflightCount() {
    lock.lock();
    try {
      return inflightQueue.size();
    } finally {
      lock.unlock();
    }
  }

  /** Returns the total number of batches (pending + inflight). */
  int totalCount() {
    lock.lock();
    try {
      return pendingQueue.size() + inflightQueue.size();
    } finally {
      lock.unlock();
    }
  }

  /** Returns the number of available permits. */
  int availablePermits() {
    return semaphore.availablePermits();
  }
}
