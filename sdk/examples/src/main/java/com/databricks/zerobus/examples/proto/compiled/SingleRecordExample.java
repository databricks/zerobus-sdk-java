package com.databricks.zerobus.examples.proto.compiled;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.examples.Record;
import com.databricks.zerobus.stream.ProtoZerobusStream;

/**
 * Example demonstrating single protobuf record ingestion with compiled proto schemas.
 *
 * <p>This example uses the fluent builder API with {@code .compiledProto()} for compiled protobuf
 * message types. The schema is known at compile time from generated Java classes.
 *
 * <p><b>Use Case:</b> Best when you have .proto files compiled into Java classes and want type-safe
 * record creation with IDE autocompletion.
 *
 * @see com.databricks.zerobus.examples.proto.dynamic.SingleRecordExample for runtime schema
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
    System.out.println("Starting compiled proto single record ingestion example...");
    System.out.println("============================================================");

    try {
      // Step 1: Initialize the SDK
      ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
      System.out.println("SDK initialized");

      // Step 2: Create a proto stream using the fluent builder API
      ProtoZerobusStream<Record.AirQuality> stream =
          sdk.streamBuilder(TABLE_NAME)
              .clientCredentials(CLIENT_ID, CLIENT_SECRET)
              .recovery(true) // Enable automatic recovery on transient failures
              .maxInflightRequests(10000) // Allow up to 10k requests in flight
              .compiledProto(Record.AirQuality.getDefaultInstance())
              .build()
              .join();
      System.out.println("Stream created: " + stream.getStreamId());

      // Step 5: Ingest records one at a time
      System.out.println("\nIngesting " + NUM_RECORDS + " proto records...");
      long startTime = System.currentTimeMillis();

      try {
        for (int i = 0; i < NUM_RECORDS; i++) {
          // Create a protobuf record using generated builder (type-safe)
          Record.AirQuality record =
              Record.AirQuality.newBuilder()
                  .setDeviceName("sensor-" + (i % 10))
                  .setTemp(20 + (i % 15))
                  .setHumidity(50 + (i % 40))
                  .build();

          // Ingest the record and get its offset ID
          long offset = stream.ingest(record);

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
        System.out.println("\n============================================================");
        System.out.println("Ingestion Summary:");
        System.out.println("  Total records: " + NUM_RECORDS);
        System.out.println("  Duration: " + String.format("%.2f", durationSeconds) + " seconds");
        System.out.println(
            "  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("============================================================");

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

    System.out.println("\nCompiled proto single record example completed successfully!");
  }
}
