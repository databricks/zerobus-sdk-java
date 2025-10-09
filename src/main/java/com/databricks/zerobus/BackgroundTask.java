package com.databricks.zerobus;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A background task that runs repeatedly until cancelled. Handles task execution, error handling,
 * and cancellation.
 */
class BackgroundTask {
  private static final Logger logger = LoggerFactory.getLogger(BackgroundTask.class);

  private final Consumer<CompletableFuture<Void>> task;
  private final Consumer<Throwable> taskFailureHandler;
  private final long delayMillis;
  private final ExecutorService executor;

  private final AtomicBoolean isActive = new AtomicBoolean(false);
  private CompletableFuture<Void> cancellationToken;

  /**
   * Creates a new background task.
   *
   * @param task The task to run repeatedly. Takes a cancellation token as parameter.
   * @param taskFailureHandler Handler for task failures
   * @param delayMillis Delay between task iterations in milliseconds
   * @param executor The executor to run the task on
   */
  BackgroundTask(
      Consumer<CompletableFuture<Void>> task,
      Consumer<Throwable> taskFailureHandler,
      long delayMillis,
      ExecutorService executor) {
    this.task = task;
    this.taskFailureHandler = taskFailureHandler;
    this.delayMillis = delayMillis;
    this.executor = executor;
  }

  /**
   * Creates a new background task with no delay.
   *
   * @param task The task to run repeatedly
   * @param taskFailureHandler Handler for task failures
   * @param executor The executor to run the task on
   */
  BackgroundTask(
      Consumer<CompletableFuture<Void>> task,
      Consumer<Throwable> taskFailureHandler,
      ExecutorService executor) {
    this(task, taskFailureHandler, 0, executor);
  }

  /**
   * Starts the background task.
   *
   * @throws IllegalStateException if the task is already running
   */
  void start() {
    boolean alreadyRunning = !isActive.compareAndSet(false, true);

    if (alreadyRunning) {
      throw new IllegalStateException("Background task is already running");
    }

    cancellationToken = new CompletableFuture<>();

    CompletableFuture.runAsync(
        () -> {
          try {
            while (!cancellationToken.isDone()) {
              try {
                task.accept(cancellationToken);

                if (delayMillis > 0) {
                  Thread.sleep(delayMillis);
                }
              } catch (Throwable e) {
                taskFailureHandler.accept(e);
              }
            }
          } catch (Throwable e) {
            logger.error("Background task failed", e);
          } finally {
            synchronized (this) {
              isActive.set(false);
              this.notifyAll();
            }
          }
        },
        executor);
  }

  /** Cancels the background task. */
  void cancel() {
    if (cancellationToken != null) {
      cancellationToken.complete(null);
    }
  }

  /** Waits until the background task has stopped. */
  void waitUntilStopped() {
    synchronized (this) {
      while (isActive.get()) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }
}
