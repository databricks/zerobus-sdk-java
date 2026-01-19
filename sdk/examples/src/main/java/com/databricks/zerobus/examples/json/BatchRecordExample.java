package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.batch.json.StringBatch;
import com.databricks.zerobus.stream.JsonZerobusStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Example demonstrating batch JSON record ingestion.
 *
 * <p>This example shows how to ingest multiple JSON records in a single batch using {@code
 * ingestBatch()}. Batching improves throughput by reducing per-record overhead.
 *
 * <p><b>Use Case:</b> Best for high-volume JSON ingestion where you can accumulate records before
 * sending. The entire batch is assigned a single offset ID and acknowledged atomically.
 */
public class BatchRecordExample {

  // Configuration - update these with your values
  private static final String SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com";
  private static final String UNITY_CATALOG_ENDPOINT =
      "https://your-workspace.cloud.databricks.com";
  private static final String TABLE_NAME = "catalog.schema.table";
  private static final String CLIENT_ID = "your-oauth-client-id";
  private static final String CLIENT_SECRET = "your-oauth-client-secret";

  // Batch configuration
  private static final int TOTAL_RECORDS = 10000;
  private static final int BATCH_SIZE = 100;

  public static void main(String[] args) {
    System.out.println("Starting JSON batch record ingestion example...");
    System.out.println("================================================");

    try {
      // Step 1: Initialize the SDK
      ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
      System.out.println("SDK initialized");

      // Step 2: Create a JSON stream using the fluent builder API
      JsonZerobusStream stream =
          sdk.streamBuilder(TABLE_NAME)
              .clientCredentials(CLIENT_ID, CLIENT_SECRET)
              .recovery(true)
              .maxInflightRequests(50000) // Higher limit for batch ingestion
              .json()
              .build()
              .join();

      // --- Optional: Custom authentication ---
      // HeadersProvider customAuth = new HeadersProvider() {
      //   @Override
      //   public java.util.Map<String, String> getHeaders() {
      //     java.util.Map<String, String> headers = new java.util.HashMap<>();
      //     headers.put("authorization", "Bearer " + getMyCustomToken());
      //     headers.put("x-databricks-zerobus-table-name", TABLE_NAME);
      //     return headers;
      //   }
      // };
      // JsonZerobusStream stream = sdk.streamBuilder(TABLE_NAME)
      //     .clientCredentials(CLIENT_ID, CLIENT_SECRET)
      //     .headersProvider(customAuth)
      //     .json()
      //     .build()
      //     .join();

      System.out.println("Stream created: " + stream.getStreamId());

      // Step 3: Ingest JSON records in batches
      System.out.println(
          "\nIngesting " + TOTAL_RECORDS + " JSON records in batches of " + BATCH_SIZE + "...");
      long startTime = System.currentTimeMillis();
      int batchCount = 0;

      try {
        for (int i = 0; i < TOTAL_RECORDS; i += BATCH_SIZE) {
          // Build a batch of JSON records
          List<String> batch = new ArrayList<>();
          int batchEnd = Math.min(i + BATCH_SIZE, TOTAL_RECORDS);

          for (int j = i; j < batchEnd; j++) {
            String jsonRecord =
                String.format(
                    "{\"device_name\": \"sensor-%d\", \"temp\": %d, \"humidity\": %d}",
                    j % 10, 20 + (j % 15), 50 + (j % 40));
            batch.add(jsonRecord);
          }

          // Ingest the entire batch at once
          Long offset = stream.ingestBatch(StringBatch.of(batch));
          batchCount++;

          // Progress indicator
          if (batchCount % 10 == 0) {
            System.out.println(
                "  Ingested "
                    + batchEnd
                    + " records ("
                    + batchCount
                    + " batches, offset: "
                    + offset
                    + ")");
          }
        }

        // Wait for all batches to be durably written
        stream.flush();

        long endTime = System.currentTimeMillis();
        double durationSeconds = (endTime - startTime) / 1000.0;
        double recordsPerSecond = TOTAL_RECORDS / durationSeconds;

        // Step 4: Close the stream
        stream.close();
        System.out.println("\nStream closed");

        // Print summary
        System.out.println("\n================================================");
        System.out.println("Ingestion Summary:");
        System.out.println("  Total records: " + TOTAL_RECORDS);
        System.out.println("  Batch size: " + BATCH_SIZE);
        System.out.println("  Total batches: " + batchCount);
        System.out.println("  Duration: " + String.format("%.2f", durationSeconds) + " seconds");
        System.out.println(
            "  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("================================================");

      } catch (Exception e) {
        System.err.println("\nError during ingestion: " + e.getMessage());
        e.printStackTrace();
        stream.close();
        System.exit(1);
      }

    } catch (ZerobusException e) {
      System.err.println("\nFailed to initialize stream: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("\nJSON batch record example completed successfully!");
  }
}
