package com.databricks.zerobus.common.http;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for HTTP operations. This abstraction allows mocking HTTP calls in tests.
 *
 * <p>Implementations should handle connection management and proper resource cleanup.
 */
public interface HttpClient {

  /**
   * Performs an HTTP POST request with form data.
   *
   * @param url The URL to post to
   * @param formData The form data (application/x-www-form-urlencoded format)
   * @param headers Additional headers to include
   * @return The response from the server
   * @throws IOException if the request fails
   */
  @Nonnull
  HttpResponse post(
      @Nonnull String url, @Nonnull String formData, @Nonnull Map<String, String> headers)
      throws IOException;

  /**
   * Performs an HTTP GET request.
   *
   * @param url The URL to get
   * @param headers Additional headers to include
   * @return The response from the server
   * @throws IOException if the request fails
   */
  @Nonnull
  HttpResponse get(@Nonnull String url, @Nonnull Map<String, String> headers) throws IOException;

  /** Response from an HTTP request. */
  class HttpResponse {
    private final int statusCode;
    private final String body;
    private final String errorBody;

    public HttpResponse(int statusCode, @Nullable String body, @Nullable String errorBody) {
      this.statusCode = statusCode;
      this.body = body;
      this.errorBody = errorBody;
    }

    /** Returns the HTTP status code. */
    public int getStatusCode() {
      return statusCode;
    }

    /** Returns the response body, or null if the request failed. */
    @Nullable public String getBody() {
      return body;
    }

    /** Returns the error body, or null if the request succeeded. */
    @Nullable public String getErrorBody() {
      return errorBody;
    }

    /** Returns true if the status code indicates success (2xx). */
    public boolean isSuccess() {
      return statusCode >= 200 && statusCode < 300;
    }
  }
}
