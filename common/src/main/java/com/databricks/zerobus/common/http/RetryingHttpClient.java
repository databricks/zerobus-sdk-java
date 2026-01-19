package com.databricks.zerobus.common.http;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * HTTP client decorator that adds retry logic for transient failures.
 *
 * <p>This client wraps another {@link HttpClient} and automatically retries requests that fail due
 * to transient errors:
 *
 * <ul>
 *   <li>5xx status codes (server errors)
 *   <li>IOException (network issues, timeouts, connection refused)
 * </ul>
 *
 * <p>Requests are NOT retried for:
 *
 * <ul>
 *   <li>4xx status codes (client errors like 400, 401, 403, 404)
 *   <li>Successful responses (2xx)
 * </ul>
 *
 * <p>Uses exponential backoff between retries (e.g., 1s, 2s, 4s).
 */
public class RetryingHttpClient implements HttpClient {

  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final long DEFAULT_INITIAL_BACKOFF_MS = 1000;

  private final HttpClient delegate;
  private final int maxRetries;
  private final long initialBackoffMs;

  /**
   * Creates a retrying HTTP client with default settings.
   *
   * @param delegate The underlying HTTP client to use
   */
  public RetryingHttpClient(@Nonnull HttpClient delegate) {
    this(delegate, DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_BACKOFF_MS);
  }

  /**
   * Creates a retrying HTTP client with custom settings.
   *
   * @param delegate The underlying HTTP client to use
   * @param maxRetries Maximum number of retry attempts (not including the initial attempt)
   * @param initialBackoffMs Initial backoff duration in milliseconds (doubles each retry)
   */
  public RetryingHttpClient(@Nonnull HttpClient delegate, int maxRetries, long initialBackoffMs) {
    this.delegate = delegate;
    this.maxRetries = maxRetries;
    this.initialBackoffMs = initialBackoffMs;
  }

  @Override
  @Nonnull
  public HttpResponse post(
      @Nonnull String url, @Nonnull String formData, @Nonnull Map<String, String> headers)
      throws IOException {
    return executeWithRetry(() -> delegate.post(url, formData, headers));
  }

  @Override
  @Nonnull
  public HttpResponse get(@Nonnull String url, @Nonnull Map<String, String> headers)
      throws IOException {
    return executeWithRetry(() -> delegate.get(url, headers));
  }

  private HttpResponse executeWithRetry(HttpOperation operation) throws IOException {
    IOException lastException = null;
    long backoffMs = initialBackoffMs;

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        HttpResponse response = operation.execute();

        if (response.isSuccess()) {
          return response;
        }

        if (response.getStatusCode() >= 500) {
          if (attempt < maxRetries) {
            sleep(backoffMs);
            backoffMs *= 2;
            continue;
          }
          return response;
        }

        return response;

      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries) {
          sleep(backoffMs);
          backoffMs *= 2;
        }
      }
    }

    throw lastException;
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @FunctionalInterface
  private interface HttpOperation {
    HttpResponse execute() throws IOException;
  }
}
