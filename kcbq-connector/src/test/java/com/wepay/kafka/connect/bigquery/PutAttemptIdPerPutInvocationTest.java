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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Storage;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import com.wepay.kafka.connect.bigquery.write.storage.StorageApiBatchModeHandler;
import com.wepay.kafka.connect.bigquery.write.storage.StorageWriteApiDefaultStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * Test 5 — Connect-level retry design goal.
 *
 * <p>Simulates two consecutive {@link BigQuerySinkTask#put(java.util.Collection)}
 * invocations with the same {@link SinkRecord} — the shape Kafka Connect produces
 * when it re-delivers records after a {@code RetriableException} bubbles out of
 * {@link BigQuerySinkTask#put(java.util.Collection)}. Each {@code put()} invocation
 * seeds a fresh ULID on {@code recordConverter.setCurrentPutAttemptId(...)}
 * ({@code BigQuerySinkTask.java:292–295}), so the resulting BigQuery rows carry
 * <b>different</b> {@code putAttemptId} values.
 *
 * <p>This is the original design goal of the {@code trackPutAttempts} feature (PR
 * #189) — this test guards against a regression that would silently reuse the same
 * ULID across put() calls.
 *
 * <p>Uses the direct streaming path (topic <b>not</b> in {@code enableBatchLoad},
 * {@code useStorageWriteAPI=false}) so no GCS or Storage Write API code path
 * intercedes.
 */
public class PutAttemptIdPerPutInvocationTest {

  private static SinkTaskPropertiesFactory propertiesFactory;
  private static final StorageWriteApiDefaultStream mockedStorageWriteApi =
      mock(StorageWriteApiDefaultStream.class);
  private static final StorageApiBatchModeHandler mockedBatchHandler =
      mock(StorageApiBatchModeHandler.class);

  private final Time time = new MockTime();

  @BeforeAll
  public static void init() {
    propertiesFactory = new SinkTaskPropertiesFactory();
  }

  @Test
  public void consecutivePutInvocationsProduceDifferentPutAttemptIds() {
    final String topic = "put-attempt-topic";
    final Map<String, String> properties = properties(topic);

    // Ensure the KafkaDataBuilder JVM-wide flag is on for this test's assertions.
    // Constructing BigQuerySinkConfig below would also do it, but doing it here
    // makes the intent explicit.
    KafkaDataBuilder.setTrackPutAttempts(true);

    // Initialise logical converters like BigQuerySinkConnector.start() would.
    BigQuerySinkConfig cfg = new BigQuerySinkConfig(properties);
    DebeziumLogicalConverters.initialize(cfg);
    KafkaLogicalConverters.initialize(cfg);

    BigQuery bigQuery = mock(BigQuery.class);
    Table mockTable = mock(Table.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mockTable);
    InsertAllResponse okResponse = mock(InsertAllResponse.class);
    when(okResponse.hasErrors()).thenReturn(false);
    when(bigQuery.insertAll(any(InsertAllRequest.class))).thenReturn(okResponse);

    Storage storage = mock(Storage.class);
    SchemaRetriever schemaRetriever = mock(SchemaRetriever.class);
    SchemaManager schemaManager = mock(SchemaManager.class);
    SinkTaskContext ctx = mock(SinkTaskContext.class);

    BigQuerySinkTask task = BigQuerySinkTaskTest.createTestTask(
        bigQuery, schemaRetriever, storage, schemaManager,
        mockedStorageWriteApi, mockedBatchHandler, time);
    task.initialize(ctx);
    task.start(properties);

    // Same record, two consecutive put() invocations. Because the record is the same
    // object, its (topic, partition, offset) is identical — the ONLY difference in
    // the resulting BigQuery row should be the putAttemptId.
    SinkRecord record = record(topic);

    task.put(Collections.singletonList(record));
    task.flush(Collections.emptyMap());

    task.put(Collections.singletonList(record));
    task.flush(Collections.emptyMap());

    ArgumentCaptor<InsertAllRequest> captor = ArgumentCaptor.forClass(InsertAllRequest.class);
    verify(bigQuery, atLeast(2)).insertAll(captor.capture());

    List<InsertAllRequest> requests = captor.getAllValues();
    Set<String> distinctIds = new LinkedHashSet<>();
    for (InsertAllRequest req : requests) {
      for (RowToInsert row : req.getRows()) {
        String id = extractPutAttemptId(row.getContent());
        assertNotNull(id, "row must carry a putAttemptId when trackPutAttempts=true");
        distinctIds.add(id);
      }
    }

    assertTrue(distinctIds.size() >= 2,
        "consecutive put() invocations must produce different putAttemptId values "
            + "(design goal of trackPutAttempts) — got: " + distinctIds);
    // Cross-check: the two IDs should not be identical.
    List<String> asList = new java.util.ArrayList<>(distinctIds);
    assertNotEquals(asList.get(0), asList.get(1),
        "the first two putAttemptId values must differ across put() invocations");
  }

  @SuppressWarnings("unchecked")
  private static String extractPutAttemptId(Map<String, Object> row) {
    Object kafka = row.get("__kafka");
    if (!(kafka instanceof Map)) {
      return null;
    }
    Object id = ((Map<String, Object>) kafka).get(
        KafkaDataBuilder.KAFKA_DATA_PUT_ATTEMPT_ID_FIELD_NAME);
    return id == null ? null : id.toString();
  }

  private Map<String, String> properties(String topic) {
    Map<String, String> props = propertiesFactory.getProperties();
    props.put(BigQuerySinkConfig.TOPICS_CONFIG, topic);
    props.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "scratch");
    props.put(BigQuerySinkConfig.KAFKA_DATA_FIELD_NAME_CONFIG, "__kafka");
    props.put(BigQuerySinkConfig.TRACK_PUT_ATTEMPTS_CONFIG, "true");
    props.put(BigQuerySinkConfig.USE_STORAGE_WRITE_API_CONFIG, "false");
    // No enableBatchLoad → direct streaming path.
    props.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");
    return props;
  }

  private SinkRecord record(String topic) {
    Schema schema = SchemaBuilder.struct()
        .field("payload", Schema.STRING_SCHEMA)
        .build();
    Struct value = new Struct(schema).put("payload", "same-record-content");
    return new SinkRecord(
        topic, 0, null, null, schema, value, 42L, 0L, TimestampType.NO_TIMESTAMP_TYPE);
  }

  // Prevent unused-import warnings for HashSet in future extensions.
  @SuppressWarnings("unused")
  private static final Set<String> UNUSED_KEEP_HASHSET_IMPORT = new HashSet<>();
}
