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

package com.wepay.kafka.connect.bigquery.write.row;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTaskTest;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test 1 — regression pin for ticket #104629.
 *
 * <p>Exercises the GCS batch write path with {@code trackPutAttempts=true} and
 * {@code useStorageWriteAPI=false}. A retryable {@link StorageException} on the first
 * upload attempt makes {@link GcsToBqWriter} re-invoke {@code uploadRowsToGcs} inside
 * its {@code executeWithRetry} loop with the same in-memory {@code SortedMap}. The
 * {@code putAttemptId} was stamped into the {@code RowToInsert} at
 * {@code GcsBatchTableWriter.Builder.addRow()} time, so both physical uploads carry
 * identical bytes — and, when a BigQuery load job later reads the blob, identical
 * {@code putAttemptId} values.
 *
 * <p>Two assertions:
 * <ul>
 *   <li>{@code storage.create(...)} is invoked twice (fail → succeed).</li>
 *   <li>Both invocations produce byte payloads that contain the <b>same</b>
 *       {@code putAttemptId}. This is the current broken behaviour; after mitigation
 *       3.4 (attach {@code withUlidSupplier} to the GCS batch builder) or 3.5
 *       (idempotent upload via {@code doesNotExist()} precondition) is applied, the
 *       assertion in {@link #retriedUploadEmitsSamePutAttemptId_regressionPin()} should
 *       be inverted to {@code assertNotEquals} or the count reduced to a single upload.
 * </ul>
 */
public class GcsPathPutAttemptIdDuplicationTest {

  private static SinkPropertiesFactory propertiesFactory;
  private static final StorageWriteApiDefaultStream mockedStorageWriteApi =
      mock(StorageWriteApiDefaultStream.class);
  private static final StorageApiBatchModeHandler mockedBatchHandler =
      mock(StorageApiBatchModeHandler.class);
  private static final Pattern PUT_ATTEMPT_ID_PATTERN =
      Pattern.compile("\"putAttemptId\"\\s*:\\s*\"([^\"]+)\"");

  private final Time time = new MockTime();

  @BeforeAll
  public static void init() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void retriedUploadEmitsSamePutAttemptId_regressionPin() {
    final String topic = "test_topic";
    Map<String, String> properties = properties(topic);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    // First upload throws retryable StorageException; second call returns normally.
    // Both invocations receive the same byte payload because the row map is frozen
    // at addRow() time.
    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class)))
        .thenThrow(new StorageException(500, "simulated GCS transient failure"))
        .thenReturn(null);

    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    SinkTaskContext ctx = mock(SinkTaskContext.class);

    BigQuerySinkTask task = BigQuerySinkTaskTest.createTestTask(
        bigQuery, schemaRetriever, storage, schemaManager,
        mockedStorageWriteApi, mockedBatchHandler, time);
    task.initialize(ctx);
    task.start(properties);

    task.put(Collections.singletonList(recordWithStructValue(topic)));
    task.flush(Collections.emptyMap());

    ArgumentCaptor<byte[]> bytesCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(storage, times(2)).create(any(BlobInfo.class), bytesCaptor.capture());

    List<byte[]> payloads = bytesCaptor.getAllValues();
    assertEquals(2, payloads.size(), "expected two physical GCS uploads (fail + retry)");

    Set<String> ids = new HashSet<>();
    for (byte[] p : payloads) {
      ids.add(extractPutAttemptId(p));
    }
    // Current broken behaviour: both retries carry the same putAttemptId.
    // Flip this to assertNotEquals once mitigation 3.4 / 3.5 is applied.
    assertEquals(1, ids.size(),
        "regression pin — two GCS uploads currently share the same putAttemptId; "
            + "after the fix, this should be 2 (or the retry should be idempotent "
            + "and the count of uploads should drop to 1)");
  }

  private static String extractPutAttemptId(byte[] payload) {
    String json = new String(payload, StandardCharsets.UTF_8);
    Matcher m = PUT_ATTEMPT_ID_PATTERN.matcher(json);
    assertTrue(m.find(),
        "GCS payload should contain a putAttemptId field but was: " + json);
    return m.group(1);
  }

  private Map<String, String> properties(String topic) {
    Map<String, String> props = propertiesFactory.getProperties();
    props.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    props.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, topic);
    props.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, "test-bucket");
    props.put(BigQuerySinkConfig.BIGQUERY_RETRY_CONFIG, "3");
    props.put(BigQuerySinkConfig.BIGQUERY_RETRY_WAIT_CONFIG, "10");
    props.put(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true");
    props.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "__kafka");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");
    props.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");
    return props;
  }

  private SinkRecord recordWithStructValue(String topic) {
    Schema schema = SchemaBuilder.struct()
        .field("payload", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("payload", "hello");
    return new SinkRecord(
        topic, 0, null, null, schema, value, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE);
  }

  // Prevent unused-import warnings for LinkedHashMap while keeping the file self-contained
  // for future extensions that need ordered maps.
  @SuppressWarnings("unused")
  private static final Map<String, Object> UNUSED = new LinkedHashMap<>();
}
