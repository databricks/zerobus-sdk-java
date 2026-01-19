package com.databricks.zerobus;

/**
 * An exception that indicates a non-retriable error has occurred.
 *
 * <p>This exception is thrown when the error is permanent and cannot be resolved by retrying.
 * Common causes include:
 *
 * <ul>
 *   <li>Invalid credentials (wrong client ID or secret)
 *   <li>Missing table or insufficient permissions
 *   <li>Schema mismatch between data and table
 *   <li>Invalid configuration parameters
 * </ul>
 *
 * <p>When this exception is thrown, the operation should not be retried without first fixing the
 * underlying issue. Contrast with {@link ZerobusException} which indicates a retriable error.
 *
 * @see ZerobusException
 */
public class NonRetriableException extends ZerobusException {

  /**
   * Constructs a new NonRetriableException with the specified detail message.
   *
   * @param message the detail message
   */
  public NonRetriableException(String message) {
    super(message);
  }

  /**
   * Constructs a new NonRetriableException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public NonRetriableException(String message, Throwable cause) {
    super(message, cause);
  }
}
