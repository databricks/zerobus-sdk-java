package com.databricks.zerobus.examples.proto.dynamic;

import com.databricks.zerobus.ZerobusSdk;
import com.databricks.zerobus.stream.ProtoZerobusStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;

/**
 * Example demonstrating single record ingestion with runtime-built proto schemas.
 *
 * <p>This example uses the fluent builder API with {@code .dynamicProto()} for runtime-created
 * protobuf descriptors. The schema is built programmatically at runtime rather than from compiled
 * .proto files.
 *
 * <p><b>Use Case:</b> Best when schemas are determined dynamically, such as:
 *
 * <ul>
 *   <li>Multi-tenant systems where each tenant has different schemas
 *   <li>Schema registry integration where schemas are fetched at runtime
 *   <li>Generic data pipelines that handle multiple message types
 * </ul>
 *
 * @see com.databricks.zerobus.examples.proto.compiled.SingleRecordExample for compile-time schema
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
    System.out.println("Starting dynamic proto single record ingestion example...");
    System.out.println("===========================================================");

    try {
      // Step 1: Build the protobuf descriptor programmatically at runtime
      // This creates a schema equivalent to:
      //   message AirQuality {
      //     optional string device_name = 1;
      //     optional int32 temp = 2;
      //     optional int32 humidity = 3;
      //   }
      Descriptors.Descriptor messageDescriptor = buildAirQualityDescriptor();
      System.out.println("Built descriptor for: " + messageDescriptor.getFullName());
      System.out.println("Fields: " + messageDescriptor.getFields().size());

      // Step 2: Initialize the SDK
      ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT);
      System.out.println("SDK initialized");

      // Step 3: Create a dynamic proto stream using the fluent builder API
      ProtoZerobusStream<DynamicMessage> stream =
          sdk.streamBuilder(TABLE_NAME)
              .clientCredentials(CLIENT_ID, CLIENT_SECRET)
              .recovery(true)
              .maxInflightRequests(10000)
              .dynamicProto(messageDescriptor)
              .build()
              .join();
      System.out.println("Stream created: " + stream.getStreamId());

      // Step 4: Ingest records one at a time
      System.out.println("\nIngesting " + NUM_RECORDS + " dynamic proto records...");
      long startTime = System.currentTimeMillis();

      try {
        for (int i = 0; i < NUM_RECORDS; i++) {
          // Create a record using DynamicMessage.Builder
          // Fields are set by field descriptor (no compile-time type safety)
          DynamicMessage record = buildDynamicRecord(messageDescriptor, i);

          // Ingest the record
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

        // Step 5: Close the stream
        stream.close();
        System.out.println("\nStream closed");

        // Print summary
        System.out.println("\n===========================================================");
        System.out.println("Ingestion Summary:");
        System.out.println("  Total records: " + NUM_RECORDS);
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

    } catch (Exception e) {
      System.err.println("\nFailed to initialize: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("\nDynamic proto single record example completed successfully!");
  }

  /**
   * Builds a protobuf descriptor programmatically at runtime.
   *
   * <p>This creates a schema equivalent to:
   *
   * <pre>
   * message AirQuality {
   *   optional string device_name = 1;
   *   optional int32 temp = 2;
   *   optional int32 humidity = 3;
   * }
   * </pre>
   *
   * @return The message descriptor
   */
  private static Descriptors.Descriptor buildAirQualityDescriptor() throws Exception {
    // Define fields
    DescriptorProtos.FieldDescriptorProto deviceNameField =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("device_name")
            .setNumber(1)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();

    DescriptorProtos.FieldDescriptorProto tempField =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("temp")
            .setNumber(2)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();

    DescriptorProtos.FieldDescriptorProto humidityField =
        DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("humidity")
            .setNumber(3)
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .build();

    // Define message type
    DescriptorProtos.DescriptorProto messageType =
        DescriptorProtos.DescriptorProto.newBuilder()
            .setName("AirQuality")
            .addField(deviceNameField)
            .addField(tempField)
            .addField(humidityField)
            .build();

    // Create file descriptor
    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("air_quality.proto")
            .addMessageType(messageType)
            .build();

    Descriptors.FileDescriptor fileDescriptor =
        Descriptors.FileDescriptor.buildFrom(
            fileDescriptorProto, new Descriptors.FileDescriptor[] {});

    return fileDescriptor.findMessageTypeByName("AirQuality");
  }

  /**
   * Builds a DynamicMessage record using the runtime-built descriptor.
   *
   * @param descriptor The message descriptor
   * @param index Record index for generating sample data
   * @return A DynamicMessage record
   */
  private static DynamicMessage buildDynamicRecord(Descriptors.Descriptor descriptor, int index) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

    // Set fields by field descriptor
    for (FieldDescriptor field : descriptor.getFields()) {
      switch (field.getName()) {
        case "device_name":
          builder.setField(field, "sensor-" + (index % 10));
          break;
        case "temp":
          builder.setField(field, 20 + (index % 15));
          break;
        case "humidity":
          builder.setField(field, 50 + (index % 40));
          break;
      }
    }

    return builder.build();
  }
}
