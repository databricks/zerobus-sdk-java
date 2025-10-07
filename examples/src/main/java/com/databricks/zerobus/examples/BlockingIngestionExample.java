package com.databricks.zerobus.examples;

import com.databricks.zerobus.*;

/**
 * Example demonstrating blocking (synchronous) record ingestion.
 *
 * <p>This example shows how to ingest records synchronously, waiting for each
 * record to be durably written before proceeding to the next one. This approach
 * provides the strongest durability guarantees but has lower throughput compared
 * to non-blocking ingestion.
 *
 * <p><b>Use Case:</b> Best for low-volume ingestion where durability is critical
 * and you need immediate confirmation of each write.
 */
public class BlockingIngestionExample {

    // Configuration - update these with your values
    private static final String SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com";
    private static final String UNITY_CATALOG_ENDPOINT = "https://your-workspace.cloud.databricks.com";
    private static final String TABLE_NAME = "catalog.schema.table";
    private static final String CLIENT_ID = "your-oauth-client-id";
    private static final String CLIENT_SECRET = "your-oauth-client-secret";

    // Number of records to ingest
    private static final int NUM_RECORDS = 1000;

    public static void main(String[] args) {
        System.out.println("Starting blocking ingestion example...");
        System.out.println("===========================================");

        try {
            // Step 1: Initialize the SDK
            ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
            System.out.println("✓ SDK initialized");

            // Step 2: Define table properties with your protobuf message type
            // Note: Replace Record.AirQuality with your own protobuf message class
            TableProperties<Record.AirQuality> tableProperties = new TableProperties<>(
                TABLE_NAME,
                Record.AirQuality.getDefaultInstance()
            );
            System.out.println("✓ Table properties configured");

            // Step 3: Create a stream with default configuration
            ZerobusStream<Record.AirQuality> stream = sdk.createStream(
                tableProperties,
                CLIENT_ID,
                CLIENT_SECRET
            );
            System.out.println("✓ Stream created: " + stream.getStreamId());

            // Step 4: Ingest records synchronously
            System.out.println("\nIngesting " + NUM_RECORDS + " records (blocking mode)...");
            long startTime = System.currentTimeMillis();
            int successCount = 0;

            try {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    // Create a record
                    Record.AirQuality record = Record.AirQuality.newBuilder()
                        .setDeviceName("sensor-" + (i % 10))
                        .setTemp(20 + (i % 15))
                        .setHumidity(50 + (i % 40))
                        .build();

                    // Ingest and wait for durability
                    IngestRecordResult result = stream.ingestRecord(record);

                    // Wait for SDK to accept the record
                    result.getRecordAccepted().join();

                    // Wait for record to be durably written
                    result.getWriteCompleted().join();

                    successCount++;

                    // Progress indicator
                    if ((i + 1) % 100 == 0) {
                        System.out.println("  Ingested " + (i + 1) + " records");
                    }
                }

                long endTime = System.currentTimeMillis();
                double durationSeconds = (endTime - startTime) / 1000.0;
                double recordsPerSecond = NUM_RECORDS / durationSeconds;

                // Step 5: Close the stream
                stream.close();
                System.out.println("\n✓ Stream closed");

                // Print summary
                System.out.println("\n===========================================");
                System.out.println("Ingestion Summary:");
                System.out.println("  Total records: " + NUM_RECORDS);
                System.out.println("  Successful: " + successCount);
                System.out.println("  Failed: " + (NUM_RECORDS - successCount));
                System.out.println("  Duration: " + String.format("%.2f", durationSeconds) + " seconds");
                System.out.println("  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
                System.out.println("===========================================");

            } catch (Exception e) {
                System.err.println("\n✗ Error during ingestion: " + e.getMessage());
                e.printStackTrace();
                stream.close();
                System.exit(1);
            }

        } catch (ZerobusException e) {
            System.err.println("\n✗ Failed to initialize stream: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("\nBlocking ingestion example completed successfully!");
    }
}
