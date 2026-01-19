package com.databricks.zerobus.common.http;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.zerobus.common.http.HttpClient.HttpResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Tests for RetryingHttpClient. */
class RetryingHttpClientTest {

  private static final Map<String, String> EMPTY_HEADERS = new HashMap<>();

  @Test
  void testSuccessOnFirstAttempt() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(200, "success", null));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(200, response.getStatusCode());
    assertEquals("success", response.getBody());
    assertEquals(1, mock.getCallCount());
  }

  @Test
  void testClientErrorNotRetried() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(400, null, "Bad Request"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(400, response.getStatusCode());
    assertEquals(1, mock.getCallCount()); // Should NOT retry
  }

  @Test
  void testUnauthorizedNotRetried() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(401, null, "Unauthorized"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(401, response.getStatusCode());
    assertEquals(1, mock.getCallCount()); // Should NOT retry
  }

  @Test
  void testForbiddenNotRetried() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(403, null, "Forbidden"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(403, response.getStatusCode());
    assertEquals(1, mock.getCallCount()); // Should NOT retry
  }

  @Test
  void testServerErrorRetried() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(500, null, "Internal Server Error"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(500, response.getStatusCode());
    assertEquals(4, mock.getCallCount()); // 1 initial + 3 retries
  }

  @Test
  void testServerErrorSucceedsAfterRetry() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    // Fail twice with 500, then succeed
    mock.setResponses(
        new HttpResponse(500, null, "Error"),
        new HttpResponse(500, null, "Error"),
        new HttpResponse(200, "success", null));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(200, response.getStatusCode());
    assertEquals("success", response.getBody());
    assertEquals(3, mock.getCallCount());
  }

  @Test
  void testServiceUnavailableRetried() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(503, null, "Service Unavailable"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 2, 1);
    HttpResponse response = client.get("http://test", EMPTY_HEADERS);

    assertEquals(503, response.getStatusCode());
    assertEquals(3, mock.getCallCount()); // 1 initial + 2 retries
  }

  @Test
  void testIOExceptionRetried() {
    MockHttpClient mock = new MockHttpClient();
    mock.setException(new IOException("Connection refused"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);

    IOException thrown =
        assertThrows(IOException.class, () -> client.post("http://test", "data", EMPTY_HEADERS));

    assertEquals("Connection refused", thrown.getMessage());
    assertEquals(4, mock.getCallCount()); // 1 initial + 3 retries
  }

  @Test
  void testIOExceptionSucceedsAfterRetry() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    // Fail twice with IOException, then succeed
    mock.setResponsesWithExceptions(
        null, // IOException
        null, // IOException
        new HttpResponse(200, "success", null));
    mock.setException(new IOException("Network error"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(200, response.getStatusCode());
    assertEquals(3, mock.getCallCount());
  }

  @Test
  void testGetMethodRetries() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponses(
        new HttpResponse(502, null, "Bad Gateway"), new HttpResponse(200, "success", null));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 1);
    HttpResponse response = client.get("http://test", EMPTY_HEADERS);

    assertEquals(200, response.getStatusCode());
    assertEquals(2, mock.getCallCount());
  }

  @Test
  void testZeroRetriesMeansOneAttempt() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(500, null, "Error"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 0, 1);
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    assertEquals(500, response.getStatusCode());
    assertEquals(1, mock.getCallCount()); // Only 1 attempt, no retries
  }

  @Test
  void testDefaultConstructorUsesDefaults() throws IOException {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(200, "success", null));

    // Uses default constructor (3 retries, 1000ms backoff)
    RetryingHttpClient client = new RetryingHttpClient(mock);
    HttpResponse response = client.get("http://test", EMPTY_HEADERS);

    assertEquals(200, response.getStatusCode());
    assertEquals(1, mock.getCallCount());
  }

  @Test
  void testThreadInterruptionDuringSleep() throws Exception {
    MockHttpClient mock = new MockHttpClient();
    mock.setResponse(new HttpResponse(500, null, "Error"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 3, 100);

    Thread testThread = Thread.currentThread();

    // Start a thread that will interrupt us during the retry sleep
    Thread interrupter =
        new Thread(
            () -> {
              try {
                Thread.sleep(50); // Wait for retry to start sleeping
                testThread.interrupt();
              } catch (InterruptedException e) {
                // ignore
              }
            });
    interrupter.start();

    // This should complete despite interruption (interrupt is handled gracefully)
    HttpResponse response = client.post("http://test", "data", EMPTY_HEADERS);

    interrupter.join(1000);

    assertEquals(500, response.getStatusCode());
    // Clear interrupted status
    Thread.interrupted();
  }

  @Test
  void testIOExceptionOnGetMethod() {
    MockHttpClient mock = new MockHttpClient();
    mock.setException(new IOException("Network timeout"));

    RetryingHttpClient client = new RetryingHttpClient(mock, 2, 1);

    IOException thrown =
        assertThrows(IOException.class, () -> client.get("http://test", EMPTY_HEADERS));

    assertEquals("Network timeout", thrown.getMessage());
    assertEquals(3, mock.getCallCount()); // 1 initial + 2 retries
  }

  /** Mock HttpClient for testing retry behavior. */
  private static class MockHttpClient implements HttpClient {
    private final AtomicInteger callCount = new AtomicInteger(0);
    private HttpResponse[] responses;
    private int responseIndex = 0;
    private IOException exception;

    void setResponse(HttpResponse response) {
      this.responses = new HttpResponse[] {response};
    }

    void setResponses(HttpResponse... responses) {
      this.responses = responses;
    }

    void setResponsesWithExceptions(HttpResponse... responses) {
      this.responses = responses;
    }

    void setException(IOException exception) {
      this.exception = exception;
    }

    int getCallCount() {
      return callCount.get();
    }

    @Override
    public HttpResponse post(String url, String formData, Map<String, String> headers)
        throws IOException {
      return execute();
    }

    @Override
    public HttpResponse get(String url, Map<String, String> headers) throws IOException {
      return execute();
    }

    private HttpResponse execute() throws IOException {
      callCount.incrementAndGet();

      if (responses != null && responseIndex < responses.length) {
        HttpResponse response = responses[responseIndex++];
        if (response != null) {
          return response;
        }
      }

      if (exception != null) {
        throw exception;
      }

      return responses[responses.length - 1];
    }
  }
}
