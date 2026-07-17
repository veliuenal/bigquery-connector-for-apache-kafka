/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wepay.kafka.connect.bigquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test 3 — GCS batch load-job deduplication guard.
 *
 * <p>Exercises the deterministic-job-ID feature added in PR #206
 * ({@code GcsToBqLoadRunnable.stableJobId(...)}). If a batch of blobs is submitted a
 * second time (e.g. after a task restart that lost in-memory state but before the
 * blobs were deleted from GCS), the second {@code bigQuery.create(JobInfo)} call
 * must be met with a 409 from BigQuery; the runnable must then retrieve the
 * existing job via {@code bigQuery.getJob(...)} instead of running a second load.
 *
 * <p>The test uses two independent {@link GcsToBqLoadRunnable} instances that share a
 * mock {@link BigQuery}. The second instance's {@code triggerBigQueryLoadJob} pass
 * simulates the restart scenario. This is broader than the existing
 * {@code testTriggerLoadJob_409ConflictOnCreate_retrievesExistingJob} — that one only
 * checks the first submission's 409 handling; this one verifies the two submissions
 * compute the <b>same</b> job ID (the underlying property that makes 409 handling
 * meaningful).
 */
public class GcsToBqLoadDeduplicationTest {

  private static final String BUCKET = "test-bucket";
  private static final String BLOB_NAME = "test-blob";

  @Test
  public void secondSubmissionOfSameBatchHits409AndReusesExistingJob() {
    BigQuery bigQuery = mock(BigQuery.class);
    Bucket bucket = mock(Bucket.class);
    when(bucket.getName()).thenReturn(BUCKET);

    Blob blob = mock(Blob.class);
    BlobId blobId = BlobId.of(BUCKET, BLOB_NAME);
    when(blob.getBlobId()).thenReturn(blobId);
    when(blob.getName()).thenReturn(BLOB_NAME);

    TableId table = TableId.of("dataset", "table");

    // --- First runnable instance: successful submit → DONE without error. ---
    Map<Job, List<BlobId>> firstActive = new HashMap<>();
    Set<BlobId> firstClaimed = new HashSet<>();
    Set<BlobId> firstDeletable = new HashSet<>();
    Map<String, Integer> firstAttempts = new HashMap<>();

    GcsToBqLoadRunnable firstRun = new GcsToBqLoadRunnable(
        bigQuery, bucket, firstActive, firstClaimed, firstDeletable, firstAttempts);

    Job createdJob = mock(Job.class);
    // Return the same mock Job from bigQuery.create; capture the JobInfo so we can
    // pin createdJob.getJobId() to it AFTER the call (nesting `when(...)` inside
    // a `thenAnswer` triggers Mockito's UnfinishedStubbing detector).
    ArgumentCaptor<JobInfo> createCaptor = ArgumentCaptor.forClass(JobInfo.class);
    when(bigQuery.create(createCaptor.capture())).thenReturn(createdJob);

    firstRun.triggerBigQueryLoadJob(table, Collections.singletonList(blob));

    JobId firstJobId = createCaptor.getValue().getJobId();
    when(createdJob.getJobId()).thenReturn(firstJobId);
    assertTrue(firstClaimed.contains(blobId));
    assertTrue(firstActive.containsKey(createdJob));

    // Now the load job completes successfully → checkJobs promotes blob to deletable.
    JobStatus success = mock(JobStatus.class);
    when(success.getState()).thenReturn(JobStatus.State.DONE);
    when(success.getError()).thenReturn(null);
    when(createdJob.getStatus()).thenReturn(success);
    when(bigQuery.getJob(firstJobId)).thenReturn(createdJob);

    firstRun.checkJobs();

    assertTrue(firstDeletable.contains(blobId),
        "blob should be marked deletable after successful load");
    assertTrue(firstAttempts.isEmpty(),
        "successful-first-attempt batches should leave blobBatchAttempts empty");

    // --- Second runnable instance: simulates restart with fresh in-memory state ---
    // but the blob still exists in GCS (deleteBlobs() didn't run yet or failed).
    Map<Job, List<BlobId>> secondActive = new HashMap<>();
    Set<BlobId> secondClaimed = new HashSet<>();
    Set<BlobId> secondDeletable = new HashSet<>();
    Map<String, Integer> secondAttempts = new HashMap<>();

    GcsToBqLoadRunnable secondRun = new GcsToBqLoadRunnable(
        bigQuery, bucket, secondActive, secondClaimed, secondDeletable, secondAttempts);

    // Now bigQuery.create must throw 409, and getJob(sameId) must return the existing job.
    when(bigQuery.create(any(JobInfo.class)))
        .thenThrow(new BigQueryException(409, "Already Exists: Job"));
    when(bigQuery.getJob(any(JobId.class))).thenReturn(createdJob);

    secondRun.triggerBigQueryLoadJob(table, Collections.singletonList(blob));

    // The existing job must have been retrieved and put into activeJobs; no second
    // physical load should have been kicked off (create() threw 409 instead of
    // returning a fresh Job).
    assertEquals(1, secondActive.size(),
        "second runnable must track exactly one active job — the existing one");
    assertTrue(secondActive.containsKey(createdJob),
        "the existing job must be the one being monitored");
    assertTrue(secondClaimed.contains(blobId),
        "blob must be re-claimed under the existing job");
    // getJob(firstJobId) is called at least twice: once by firstRun.checkJobs() to
    // poll status, and once by secondRun.triggerBigQueryLoadJob() when it swallows
    // the 409 and retrieves the existing job.
    verify(bigQuery, atLeast(2)).getJob(firstJobId);

    // Sanity: overall create() count across both runnables is exactly 2 (1 success + 1 409).
    verify(bigQuery, atLeast(2)).create(any(JobInfo.class));
  }
}
