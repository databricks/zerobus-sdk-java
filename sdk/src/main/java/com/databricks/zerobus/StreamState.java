package com.databricks.zerobus;

/**
 * Represents the lifecycle state of a ZerobusStream.
 *
 * <p>State transitions typically follow this pattern:
 *
 * <pre>
 * UNINITIALIZED → OPENED → FLUSHING → CLOSED
 *                    ↓
 *                 PAUSED (on server close signal, waiting for acks)
 *                    ↓
 *                RECOVERING (on failure or after pause timeout)
 *                    ↓
 *                OPENED or FAILED
 * </pre>
 */
public enum StreamState {
  /** Stream created but not yet initialized */
  UNINITIALIZED,

  /** Stream is open and accepting records */
  OPENED,

  /** Stream is flushing pending records before closing */
  FLUSHING,

  /**
   * Stream is paused due to server close signal.
   *
   * <p>During this state:
   *
   * <ul>
   *   <li>The sender task is paused (stops sending batches to server)
   *   <li>Ingestion can continue - records queue in the landing zone
   *   <li>The receiver continues processing acks for in-flight records
   *   <li>Recovery triggers after watermark batches are acked or timeout
   * </ul>
   *
   * <p>Records ingested during PAUSED state will be sent after recovery completes.
   */
  PAUSED,

  /** Stream is recovering from a failure (automatic retry in progress) */
  RECOVERING,

  /** Stream has been gracefully closed */
  CLOSED,

  /** Stream has failed and cannot be recovered */
  FAILED
}
