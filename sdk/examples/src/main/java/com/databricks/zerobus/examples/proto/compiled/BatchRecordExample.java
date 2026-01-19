package com.databricks.zerobus.examples.proto.compiled;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.batch.proto.MessageBatch;
import com.databricks.zerobus.examples.Record;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Example demonstrating batch protobuf record ingestion with compiled proto schemas.
 *
 * <p>This example uses the fluent builder API with {@code .compiledProto()} for compiled protobuf
 * message types. The schema is known at compile time from generated Java classes.
 *
 * <p><b>Use Case:</b> Best for high-volume ingestion with type-safe record creation when you have
 * .proto files compiled into Java classes.
 *
 * @see com.databricks.zerobus.examples.proto.dynamic.BatchRecordExample for runtime schema
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
    System.out.println("Starting compiled proto batch record ingestion example...");
    System.out.println("===========================================================");

    try {
      // Step 1: Initialize the SDK
      ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
      System.out.println("SDK initialized");

      // Step 2: Create a proto stream using the fluent builder API
      ProtoZerobusStream<Record.AirQuality> stream =
          sdk.streamBuilder(TABLE_NAME)
              .clientCredentials(CLIENT_ID, CLIENT_SECRET)
              .recovery(true)
              .maxInflightRequests(50000) // Higher limit for batch ingestion
              .compiledProto(Record.AirQuality.getDefaultInstance())
              .build()
              .join();
      System.out.println("Stream created: " + stream.getStreamId());

      // Step 3: Ingest records in batches
      System.out.println(
          "\nIngesting " + TOTAL_RECORDS + " proto records in batches of " + BATCH_SIZE + "...");
      long startTime = System.currentTimeMillis();
      int batchCount = 0;

      try {
        for (int i = 0; i < TOTAL_RECORDS; i += BATCH_SIZE) {
          // Build a batch of records using generated builders (type-safe)
          List<Record.AirQuality> batch = new ArrayList<>();
          int batchEnd = Math.min(i + BATCH_SIZE, TOTAL_RECORDS);

          for (int j = i; j < batchEnd; j++) {
            Record.AirQuality record =
                Record.AirQuality.newBuilder()
                    .setDeviceName("sensor-" + (j % 10))
                    .setTemp(20 + (j % 15))
                    .setHumidity(50 + (j % 40))
                    .build();
            batch.add(record);
          }

          // Ingest the entire batch at once
          Long offset = stream.ingestBatch(MessageBatch.of(batch));
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
        System.out.println("\n===========================================================");
        System.out.println("Ingestion Summary:");
        System.out.println("  Total records: " + TOTAL_RECORDS);
        System.out.println("  Batch size: " + BATCH_SIZE);
        System.out.println("  Total batches: " + batchCount);
        System.out.println("  Duration: " + String.format("%.2f", durationSeconds) + " seconds");
        System.out.println(
            "  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("===========================================================");

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

    System.out.println("\nCompiled proto batch record example completed successfully!");
  }
}
