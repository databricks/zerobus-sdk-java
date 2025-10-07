package com.databricks.zerobus.examples;

import com.databricks.zerobus.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Example demonstrating non-blocking (asynchronous) record ingestion.
 *
 * <p>This example shows how to ingest records asynchronously, allowing maximum
 * throughput by not waiting for each record to complete before submitting the next.
 * The SDK manages buffering and flow control automatically.
 *
 * <p><b>Use Case:</b> Best for high-volume ingestion where maximum throughput is
 * important. Records are still durably written, but acknowledgments are handled
 * asynchronously.
 */
public class NonBlockingIngestionExample {

    // Configuration - update these with your values
    private static final String SERVER_ENDPOINT = "your-shard-id.zerobus.region.cloud.databricks.com";
    private static final String UNITY_CATALOG_ENDPOINT = "https://your-workspace.cloud.databricks.com";
    private static final String TABLE_NAME = "catalog.schema.table";
    private static final String CLIENT_ID = "your-oauth-client-id";
    private static final String CLIENT_SECRET = "your-oauth-client-secret";

    // Number of records to ingest
    private static final int NUM_RECORDS = 100_000;

    public static void main(String[] args) {
        System.out.println("Starting non-blocking ingestion example...");
        System.out.println("===========================================");

        try {
            // Step 1: Initialize the SDK
            ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
            System.out.println("✓ SDK initialized");

            // Step 2: Configure stream options with ack callback
            StreamConfigurationOptions options = StreamConfigurationOptions.builder()
                .setMaxInflightRecords(50_000)  // Allow 50k records in flight
                .setRecovery(true)               // Enable automatic recovery
                .setAckCallback(createAckCallback())  // Track acknowledgments
                .build();
            System.out.println("✓ Stream configuration created");

            // Step 3: Define table properties with your protobuf message type
            // Note: Replace Record.AirQuality with your own protobuf message class
            TableProperties<Record.AirQuality> tableProperties = new TableProperties<>(
                TABLE_NAME,
                Record.AirQuality.getDefaultInstance()
            );
            System.out.println("✓ Table properties configured");

            // Step 4: Create a stream
            ZerobusStream<Record.AirQuality> stream = sdk.createStream(
                tableProperties,
                CLIENT_ID,
                CLIENT_SECRET,
                options
            );
            System.out.println("✓ Stream created: " + stream.getStreamId());

            // Step 5: Ingest records asynchronously
            System.out.println("\nIngesting " + NUM_RECORDS + " records (non-blocking mode)...");
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            long startTime = System.currentTimeMillis();

            try {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    // Create a record with varying data
                    Record.AirQuality record = Record.AirQuality.newBuilder()
                        .setDeviceName("sensor-" + (i % 10))
                        .setTemp(20 + (i % 15))
                        .setHumidity(50 + (i % 40))
                        .build();

                    // Ingest record (non-blocking)
                    IngestRecordResult result = stream.ingestRecord(record);

                    // Wait for SDK to accept (this is fast, just queue check)
                    result.getRecordAccepted().join();

                    // Collect futures to wait for durability later
                    futures.add(result.getWriteCompleted());

                    // Progress indicator
                    if ((i + 1) % 10000 == 0) {
                        System.out.println("  Submitted " + (i + 1) + " records");
                    }
                }

                long submitEndTime = System.currentTimeMillis();
                double submitDuration = (submitEndTime - startTime) / 1000.0;

                System.out.println("\n✓ All records submitted in " +
                    String.format("%.2f", submitDuration) + " seconds");

                // Step 6: Flush and wait for all records to be durably written
                System.out.println("\nFlushing stream and waiting for durability...");
                stream.flush();

                // Wait for all futures to complete
                CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
                );
                allFutures.join();

                long endTime = System.currentTimeMillis();
                double totalDuration = (endTime - startTime) / 1000.0;
                double recordsPerSecond = NUM_RECORDS / totalDuration;

                System.out.println("✓ All records durably written");

                // Step 7: Close the stream
                stream.close();
                System.out.println("✓ Stream closed");

                // Print summary
                System.out.println("\n===========================================");
                System.out.println("Ingestion Summary:");
                System.out.println("  Total records: " + NUM_RECORDS);
                System.out.println("  Submit time: " + String.format("%.2f", submitDuration) + " seconds");
                System.out.println("  Total time: " + String.format("%.2f", totalDuration) + " seconds");
                System.out.println("  Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
                System.out.println("  Average latency: " +
                    String.format("%.2f", (totalDuration * 1000.0) / NUM_RECORDS) + " ms/record");
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

        System.out.println("\nNon-blocking ingestion example completed successfully!");
    }

    /**
     * Creates an acknowledgment callback that logs progress.
     *
     * @return Consumer that handles acknowledgment responses
     */
    private static Consumer<IngestRecordResponse> createAckCallback() {
        return response -> {
            long offset = response.getDurabilityAckUpToOffset();
            // Log every 10000 records
            if (offset % 10000 == 0) {
                System.out.println("  Acknowledged up to offset: " + offset);
            }
        };
    }
}
