package com.databricks.zerobus;

/**
 * Base exception class for all Zerobus SDK errors.
 *
 * <p>This is an unchecked exception (extends {@link RuntimeException}). Callers can catch this
 * exception or let it propagate up the call stack.
 *
 * <p>The SDK throws two types of exceptions:
 *
 * <ul>
 *   <li>{@link ZerobusException} - Retriable errors (network issues, temporary server errors)
 *   <li>{@link NonRetriableException} - Non-retriable errors (invalid credentials, missing table)
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try {
 *     stream.ingest(record);
 * } catch (NonRetriableException e) {
 *     // Fatal error - do not retry
 *     logger.error("Non-retriable error", e);
 *     throw e;
 * } catch (ZerobusException e) {
 *     // Retriable error - can retry with backoff
 *     logger.warn("Retriable error, will retry", e);
 * }
 * }</pre>
 */
public class ZerobusException extends RuntimeException {

  /**
   * Constructs a new ZerobusException with the specified detail message.
   *
   * @param message the detail message
   */
  public ZerobusException(String message) {
    super(message);
  }

  /**
   * Constructs a new ZerobusException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public ZerobusException(String message, Throwable cause) {
    super(message, cause);
  }
}
