package com.databricks.zerobus.batch;

/**
 * Base interface for all batch types used in stream ingestion.
 *
 * <p>Batches wrap collections of records for atomic ingestion. All records in a batch are assigned
 * a single offset ID and acknowledged together.
 *
 * @param <R> The record type contained in the batch
 * @see PrimaryBatch
 * @see SecondaryBatch
 */
public interface Batch<R> extends Iterable<R> {}
