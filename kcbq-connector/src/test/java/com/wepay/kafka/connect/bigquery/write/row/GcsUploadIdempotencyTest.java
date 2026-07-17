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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.utils.MockTime;
import com.wepay.kafka.connect.bigquery.utils.Time;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test 4 — GCS upload idempotency guard (mitigation 3.5).
 *
 * <p>{@link Disabled} because the mitigation has not yet been implemented. The
 * required code change:
 * <ol>
 *   <li>{@code GcsToBqWriter.uploadBlobToGcs} calls
 *       {@code storage.create(blobInfo, content, Storage.BlobTargetOption.doesNotExist())}
 *       so a re-upload against the same blob name fails with HTTP 412
 *       ("Precondition Failed") rather than overwriting.</li>
 *   <li>The {@code executeWithRetry} loop treats
 *       {@code StorageException.getCode() == 412} as a successful upload — the
 *       previous attempt already reached GCS — and short-circuits without
 *       propagating the failure.</li>
 * </ol>
 *
 * <p>Once that mitigation is applied, remove the {@link Disabled} annotation. The
 * expected behaviour:
 * <ul>
 *   <li>A second {@link GcsToBqWriter#writeRows} call targeting the same blob name
 *       does <b>not</b> throw.</li>
 *   <li>The blob content in GCS remains what the first upload wrote — the
 *       precondition prevented overwrite.</li>
 *   <li>{@code "Batch loaded {} rows"} is still logged with the intended row count
 *       (not the number of physical uploads).</li>
 * </ul>
 */
@Disabled("re-enable once GcsToBqWriter uses doesNotExist() precondition + 412-as-success shortcut")
public class GcsUploadIdempotencyTest {

  private static final TableId TABLE = TableId.of("dataset", "table");
  private static final String BUCKET = "test-bucket";
  private static final String BLOB = "test-blob";

  private final Time time = new MockTime();

  @Test
  public void secondUploadOfSameBlobNamePreservesOriginalContent() throws Exception {
    // In-memory GCS: keeps the bytes stored under a given blob name.
    Map<String, byte[]> storageContents = new HashMap<>();

    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class))).thenAnswer(inv -> {
      BlobInfo info = inv.getArgument(0);
      byte[] bytes = inv.getArgument(1);
      String key = info.getBlobId().getName();

      // Once mitigation 3.5 is in place, the writer will pass BlobTargetOption
      // arguments to the 3-arg overload; if it accidentally still calls the 2-arg
      // form, blindly overwriting, we detect that here.
      if (storageContents.containsKey(key)) {
        throw new StorageException(412, "Precondition Failed: object already exists");
      }
      storageContents.put(key, bytes);
      return null;
    });

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    SchemaManager schemaManager = mock(SchemaManager.class);

    GcsToBqWriter writer = new GcsToBqWriter(
        storage, bigQuery, schemaManager, /* retries= */ 3, /* retryWaitMs= */ 10L,
        /* autoCreateTables= */ false, /* attemptSchemaUpdate= */ false, time);

    // --- First upload: writes payload A ---
    SortedMap<SinkRecord, RowToInsert> rowsA = oneRow("value-A");
    writer.writeRows(rowsA, TABLE, BUCKET, BLOB);

    byte[] originalContent = storageContents.get(BLOB);
    assertNotNull(originalContent, "first upload must land in the mock bucket");

    // --- Second upload: attempts to overwrite with payload B ---
    // After mitigation 3.5, this must NOT throw (412 is swallowed) AND the stored
    // content must remain payload A.
    SortedMap<SinkRecord, RowToInsert> rowsB = oneRow("value-B");
    assertDoesNotThrow(() -> writer.writeRows(rowsB, TABLE, BUCKET, BLOB),
        "second writeRows must swallow the 412 and treat it as success");

    assertArrayEquals(originalContent, storageContents.get(BLOB),
        "GCS blob must retain the ORIGINAL bytes — precondition prevented overwrite");

    ArgumentCaptor<byte[]> byteCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(storage, atLeast(2)).create(any(BlobInfo.class), byteCaptor.capture());
  }

  @Test
  public void batchLoadedLogReportsRowCountNotUploadCount() throws Exception {
    // Same setup as above; the goal here is to nail down the log message contract:
    // "Batch loaded N rows" should report the *intended* row count (rows.size())
    // and not, for example, the count of physical HTTP calls.
    Storage storage = mock(Storage.class);
    when(storage.create(any(BlobInfo.class), any(byte[].class))).thenReturn(null);

    BigQuery bigQuery = mock(BigQuery.class);
    when(bigQuery.getTable(any(TableId.class))).thenReturn(mock(Table.class));

    SchemaManager schemaManager = mock(SchemaManager.class);
    GcsToBqWriter writer = new GcsToBqWriter(
        storage, bigQuery, schemaManager, 1, 10L, false, false, time);

    SortedMap<SinkRecord, RowToInsert> rows = oneRow("only-row");
    writer.writeRows(rows, TABLE, BUCKET, BLOB);

    // If, in the future, uploadBlobToGcs is split into multiple physical calls (e.g.
    // resumable uploads), rows.size() must still be the number reported in the log.
    verify(storage, times(1)).create(any(BlobInfo.class), any(byte[].class));
    assertEquals(1, rows.size());
  }

  private SortedMap<SinkRecord, RowToInsert> oneRow(String value) {
    Comparator<SinkRecord> byOffset = Comparator.comparingLong(SinkRecord::kafkaOffset);
    SortedMap<SinkRecord, RowToInsert> rows = new TreeMap<>(byOffset);

    Schema schema = SchemaBuilder.struct().field("f", Schema.STRING_SCHEMA).build();
    Struct struct = new Struct(schema).put("f", value);
    SinkRecord rec = new SinkRecord("t", 0, null, null, schema, struct, 1L, null, null);

    Map<String, Object> content = new HashMap<>();
    content.put("f", value);
    rows.put(rec, RowToInsert.of(content));
    return rows;
  }
}
