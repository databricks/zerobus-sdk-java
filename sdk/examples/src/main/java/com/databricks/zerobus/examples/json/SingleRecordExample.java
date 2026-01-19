package com.databricks.zerobus.examples.json;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.stream.JsonZerobusStream;

/**
 * Example demonstrating single JSON record ingestion.
 *
 * <p>This example shows how to ingest JSON records one at a time. JSON ingestion is useful when:
 *
 * <ul>
 *   <li>Your data is already in JSON format
 *   <li>You don't want to define protobuf schemas
 *   <li>You need schema flexibility at the cost of type safety
 * </ul>
 *
 * <p><b>Note:</b> JSON streams require the table schema to be defined in the catalog, and JSON
 * records must match the expected schema.
 */
public class SingleRecordExample {

  // Configuration - update these with your values
  private static final String SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com";
  private static final String UNITY_CATALOG_ENDPOINT =
      "https://your-workspace.cloud.databricks.com";
  private static final String TABLE_NAME = "catalog.schema.table";
  private static final String CLIENT_ID = "your-oauth-client-id";
  private static final String CLIENT_SECRET = "your-oauth-client-secret";

  // Number of records to ingest
  private static final int NUM_RECORDS = 1000;

  public static void main(String[] args) {
    System.out.println("Starting JSON single record ingestion example...");
    System.out.println("=================================================");

    try {
      // Step 1: Initialize the SDK
      ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
      System.out.println("SDK initialized");

      // Step 2: Create a JSON stream using the fluent builder API
      JsonZerobusStream stream =
          sdk.streamBuilder(TABLE_NAME)
              .clientCredentials(CLIENT_ID, CLIENT_SECRET)
              .recovery(true)
              .maxInflightRequests(10000)
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

      // Step 5: Ingest JSON records one at a time
      System.out.println("\nIngesting " + NUM_RECORDS + " JSON records...");
      long startTime = System.currentTimeMillis();

      try {
        for (int i = 0; i < NUM_RECORDS; i++) {
          // Create a JSON record
          // Note: The JSON structure must match your table schema
          String jsonRecord =
              String.format(
                  "{\"device_name\": \"sensor-%d\", \"temp\": %d, \"humidity\": %d}",
                  i % 10, // device_name
                  20 + (i % 15), // temp
                  50 + (i % 40) // humidity
                  );

          // Ingest the JSON record and get its offset ID
          long offset = stream.ingest(jsonRecord);

          // Progress indicator
          if ((i + 1) % 100 == 0) {
            System.out.println("  Ingested " + (i + 1) + " records (last offset: " + offset + ")");
          }
        }

        // Wait for all records to be durably written
        stream.flush();

        long endTime = System.currentTimeMillis();
        double durationSeconds = (endTime - startTime) / 1000.0;
        double recordsPerSecond = NUM_RECORDS / durationSeconds;

        // Step 6: Close the stream
        stream.close();
        System.out.println("\nStream closed");

        // Print summary
        System.out.println("\n=================================================");
        System.out.println("Ingestion Summary:");
        System.out.println("  Total records: " + NUM_RECORDS);
        System.out.println("  Duration: " + String.format("%.2f", durationSeconds) + " seconds");
        System.out.println(
            "  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("=================================================");

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

    System.out.println("\nJSON single record example completed successfully!");
  }
}
