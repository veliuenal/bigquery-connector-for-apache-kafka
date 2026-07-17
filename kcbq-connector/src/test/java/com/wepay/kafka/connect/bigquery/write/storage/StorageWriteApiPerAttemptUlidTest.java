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

package com.wepay.kafka.connect.bigquery.write.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.wepay.kafka.connect.bigquery.ErrantRecordHandler;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.SinkRecordConverter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test 2 — positive regression pin for PR #206.
 *
 * <p>Verifies that when the Storage Write API retries a batch internally (i.e. the
 * outer {@code while (!batch.isEmpty())} loop in
 * {@link StorageWriteApiBase#initializeAndWriteRecords} re-invokes
 * {@code writeBatch} after a {@code RetryException}), the per-attempt ULID refresh
 * at {@code StorageWriteApiBase.java:265–281} generates a fresh {@code putAttemptId}
 * before each {@code appendRows} call.
 *
 * <p>The stub {@link StreamWriter}:
 * <ul>
 *   <li>1st call → returns a future that throws an {@link ExecutionException}
 *       wrapping a retryable gRPC status. This triggers the outer catch in
 *       {@code writeBatch}, which sets the retry exception then throws
 *       {@code RetryException}, causing {@code initializeAndWriteRecords} to loop
 *       around and re-invoke {@code writeBatch} with a fresh ULID.</li>
 *   <li>2nd call → returns a successful {@link AppendRowsResponse}.</li>
 * </ul>
 *
 * <p>Both {@link JSONArray} payloads are captured; the assertion is that the two
 * captured rows contain <b>different</b> {@code putAttemptId} values inside their
 * {@code __kafka} sub-object.
 */
public class StorageWriteApiPerAttemptUlidTest {

  private static final String TABLE = "storage-write-api-per-attempt-ulid-test";
  private static final String DATASET = "kcbq-test";
  private static final String PROJECT = "test-project";
  private static final String KAFKA_DATA_FIELD = "__kafka";

  private final PartitionedTableId table = new PartitionedTableId.Builder(DATASET, TABLE)
      .setProject(PROJECT)
      .build();

  private StorageWriteApiDefaultStream stream;
  private RecordingStreamWriter recordingWriter;
  private SinkRecordConverter recordConverter;

  @BeforeEach
  public void setUp() {
    stream = mock(StorageWriteApiDefaultStream.class, CALLS_REAL_METHODS);
    stream.time = new MockTime();
    stream.schemaManager = mock(SchemaManager.class);
    stream.errantRecordHandler = mock(ErrantRecordHandler.class);
    // Retry budget: 1 retry is enough to see two attempts.
    setField(stream, "retry", 3);
    setField(stream, "retryWait", 1L);

    recordingWriter = new RecordingStreamWriter();
    // Bypass streamWriter() — no real BigQuery/JsonStreamWriter needed.
    doReturn(recordingWriter).when(stream)
        .streamWriter(any(PartitionedTableId.class), any(), anyList());

    // Ensure the static flag inside KafkaDataBuilder is on — mirrors what happens
    // during BigQuerySinkConfig construction with trackPutAttempts=true.
    KafkaDataBuilder.setTrackPutAttempts(true);

    recordConverter = new SinkRecordConverter(taskConfig(), null, null);
  }

  @Test
  public void appendRowsRetryReceivesFreshPutAttemptId() throws Exception {
    Supplier<String> ulidSupplier = new Supplier<String>() {
      private final AtomicInteger counter = new AtomicInteger();

      @Override
      public String get() {
        return "ULID-" + counter.incrementAndGet();
      }
    };

    List<ConvertedRecord> rows = Collections.singletonList(
        new ConvertedRecord(sinkRecord(), new JSONObject()));

    stream.initializeAndWriteRecords(table, rows, /* streamName= */ null,
        recordConverter, ulidSupplier);

    assertEquals(2, recordingWriter.calls.size(),
        "writeBatch should have been invoked twice (fail + retry)");

    String firstId = extractPutAttemptId(recordingWriter.calls.get(0));
    String secondId = extractPutAttemptId(recordingWriter.calls.get(1));

    assertNotEquals(firstId, secondId,
        "each write attempt must receive a fresh putAttemptId — this is what PR #206 guarantees");
    assertEquals("ULID-1", firstId);
    assertEquals("ULID-2", secondId);
  }

  /**
   * StreamWriter stub that fails on the first append (via a retryable gRPC status
   * bubbling through {@code ExecutionException}), then succeeds. The JSONArray
   * argument to each call is captured for later assertion.
   */
  private static final class RecordingStreamWriter implements StreamWriter {
    final java.util.List<JSONArray> calls = new java.util.ArrayList<>();
    private int invocations = 0;

    @SuppressWarnings("unchecked")
    @Override
    public ApiFuture<AppendRowsResponse> appendRows(JSONArray rows) {
      calls.add(cloneJsonArray(rows));
      invocations++;
      ApiFuture<AppendRowsResponse> future =
          (ApiFuture<AppendRowsResponse>) mock(ApiFuture.class);
      try {
        if (invocations == 1) {
          // Message must contain "StreamWriterClosedException" so
          // BigQueryStorageWriteApiErrorResponses.isStreamClosed(...) returns true.
          // That branch calls writer.refresh() and throws RetryException — exactly
          // the path we want the retry loop to take (rather than failTask()).
          when(future.get()).thenThrow(new ExecutionException(
              new Throwable("StreamWriterClosedException due to FAILED_PRECONDITION")));
        } else {
          when(future.get()).thenReturn(AppendRowsResponse.newBuilder()
              .setAppendResult(AppendRowsResponse.AppendResult.getDefaultInstance())
              .build());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return future;
    }

    @Override
    public void refresh() {
    }

    @Override
    public void onSuccess() {
    }

    @Override
    public String streamName() {
      return "test-stream";
    }
  }

  private static JSONArray cloneJsonArray(JSONArray src) {
    return new JSONArray(src.toString());
  }

  private static String extractPutAttemptId(JSONArray rows) {
    assertFalse(rows.isEmpty(), "captured row batch should be non-empty");
    JSONObject row = rows.getJSONObject(0);
    assertTrue(row.has(KAFKA_DATA_FIELD),
        "row should carry the kafkaDataField (__kafka) sub-record");
    JSONObject kafka = row.getJSONObject(KAFKA_DATA_FIELD);
    assertTrue(kafka.has(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME),
        "kafka data record should carry a putAttemptId");
    return kafka.getString(KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME);
  }

  private SinkRecord sinkRecord() {
    Schema schema = SchemaBuilder.struct().field("f", Schema.STRING_SCHEMA).build();
    Struct value = new Struct(schema).put("f", "v");
    return new SinkRecord("topic", 0, null, null, schema, value, 42L);
  }

  private BigQuerySinkTaskConfig taskConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(BigQuerySinkConfig.PROJECT_CONFIG, PROJECT);
    props.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, DATASET);
    props.put(BigQuerySinkConfig.KEYFILE_CONFIG, "key.json");
    props.put(BigQuerySinkConfig.TOPICS_CONFIG, "topic");
    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "false");
    props.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, KAFKA_DATA_FIELD);
    props.put(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "true");
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
    props.put(BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG, "10");
    props.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");
    return new BigQuerySinkTaskConfig(props);
  }

  private static void setField(Object target, String name, Object value) {
    try {
      java.lang.reflect.Field f = findField(target.getClass(), name);
      f.setAccessible(true);
      f.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static java.lang.reflect.Field findField(Class<?> c, String name)
      throws NoSuchFieldException {
    Class<?> current = c;
    while (current != null) {
      try {
        return current.getDeclaredField(name);
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }

  // Suppress unused-import complaints where the compiler is finicky.
  @SuppressWarnings("unused")
  private static final Map<String, Object> UNUSED_KEEP_LINKED_HASHMAP_IMPORT = new LinkedHashMap<>();
  @SuppressWarnings("unused")
  private static final List<Object> UNUSED_KEEP_ARRAYS_IMPORT = Arrays.asList();
}
