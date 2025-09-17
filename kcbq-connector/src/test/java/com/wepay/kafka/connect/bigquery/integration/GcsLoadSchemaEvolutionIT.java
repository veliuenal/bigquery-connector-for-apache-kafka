/*
 * Copyright 2024 Copyright 2022 Aiven Oy and
 * bigquery-connector-for-apache-kafka project contributors
 *
 * This software contains code derived from the Confluent BigQuery
 * Kafka Connector, Copyright Confluent, Inc, which in turn
 * contains code derived from the WePay BigQuery Kafka Connector,
 * Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.integration;

import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.integration.utils.TableClearer;
import com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag("integration")
public class GcsLoadSchemaEvolutionIT extends BaseConnectorIT {

  private static final int TASKS_MAX = 2;
  private static final long LOAD_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);

  private BigQuery bigQuery;
  private Converter valueConverter;
  private String connectorName;
  private String topic;
  private String tableName;
  private String bucketName;
  private String folderName;

  @BeforeEach
  void setup(TestInfo testInfo) {
    String method = testInfo.getTestMethod().map(Method::getName).orElse("gcs-load");
    connectorName = String.format("gcs-load-schema-%s-%d", method, Instant.now().toEpochMilli());
    topic = suffixedTableOrTopic(String.format("gcs-load-%s", method));
    tableName = sanitizedTable(topic);
    bucketName = gcsBucket();
    folderName = buildFolderName(method);

    startConnect();
    bigQuery = newBigQuery();

    connect.kafka().createTopic(topic, 1);
    TableClearer.clearTables(bigQuery, dataset(), tableName);

    valueConverter = new JsonConverter();
    Map<String, Object> converterProps = new HashMap<>();
    converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
    valueConverter.configure(converterProps, false);
  }

  @AfterEach
  void cleanup() {
    if (connect != null && connectorName != null) {
      try {
        connect.deleteConnector(connectorName);
      } catch (Exception ignored) {
      }
    }
    stopConnect();
    bigQuery = null;
    valueConverter = null;
  }

  @Test
  public void testAddsNewFieldWithAutodetect() throws Exception {
    Map<String, String> props = connectorProps(true, false, true);
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    Schema schemaV1 = SchemaBuilder.struct()
        .name("schemaV1")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct recordV1 = new Struct(schemaV1)
        .put("id", 1L)
        .put("name", "Alice");

    Schema schemaV2 = SchemaBuilder.struct()
        .name("schemaV2")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("nickname", SchemaBuilder.string().optional().build())
        .build();
    Struct recordV2 = new Struct(schemaV2)
        .put("id", 2L)
        .put("name", "Bob")
        .put("nickname", "Bobby");

    produceRecord(schemaV1, recordV1);
    produceRecord(schemaV2, recordV2);

    waitForCommittedRecords(connectorName, topic, 2, TASKS_MAX);
    waitForRows(2);

    Table table = bigQuery.getTable(TableId.of(dataset(), tableName));
    assertNotNull(table, "BigQuery table was not created");
    TableDefinition definition = table.getDefinition();
    FieldList fields = definition.getSchema().getFields();
    Field nicknameField = fields.get("nickname");
    assertNotNull(nicknameField, "New column should exist in table schema");
    assertEquals(Field.Mode.NULLABLE, nicknameField.getMode(), "New column must be nullable");

    List<List<Object>> rows = readAllRows(bigQuery, tableName, "id");
    assertEquals(2, rows.size());
    assertEquals(Arrays.asList(1L, "Alice", null), rows.get(0));
    assertEquals(Arrays.asList(2L, "Bob", "Bobby"), rows.get(1));
  }

  @Test
  public void testRelaxesRequiredFieldWithAutodetect() throws Exception {
    Map<String, String> props = connectorProps(false, true, true);
    connect.configureConnector(connectorName, props);
    waitForConnectorToStart(connectorName, TASKS_MAX);

    Schema requiredSchema = SchemaBuilder.struct()
        .name("requiredSchema")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();
    Struct requiredRecord = new Struct(requiredSchema)
        .put("id", 1L)
        .put("name", "Initial");

    Schema relaxedSchema = SchemaBuilder.struct()
        .name("relaxedSchema")
        .field("id", Schema.INT64_SCHEMA)
        .field("name", SchemaBuilder.string().optional().build())
        .build();
    Struct relaxedRecord = new Struct(relaxedSchema)
        .put("id", 2L)
        .put("name", null);

    produceRecord(requiredSchema, requiredRecord);
    produceRecord(relaxedSchema, relaxedRecord);

    waitForCommittedRecords(connectorName, topic, 2, TASKS_MAX);
    waitForRows(2);

    Table table = bigQuery.getTable(TableId.of(dataset(), tableName));
    assertNotNull(table, "BigQuery table was not created");
    Field nameField = table.getDefinition().getSchema().getFields().get("name");
    assertNotNull(nameField, "Existing column should still exist");
    assertEquals(Field.Mode.NULLABLE, nameField.getMode(), "Existing column should be relaxed to NULLABLE");

    List<List<Object>> rows = readAllRows(bigQuery, tableName, "id");
    assertEquals(2, rows.size());
    assertEquals(Arrays.asList(1L, "Initial"), rows.get(0));
    assertEquals(Arrays.asList(2L, null), rows.get(1));
  }

  private Map<String, String> connectorProps(boolean allowNew, boolean allowRelax, boolean autodetect) {
    Map<String, String> props = baseConnectorProps(TASKS_MAX);
    props.put(TOPICS_CONFIG, topic);
    props.put(KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(KEY_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    props.put(VALUE_CONVERTER_CLASS_CONFIG + "." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    props.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    props.put(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG, IdentitySchemaRetriever.class.getName());
    props.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "true");
    props.put(BigQuerySinkConfig.ENABLE_BATCH_CONFIG, topic);
    props.put(BigQuerySinkConfig.BATCH_LOAD_INTERVAL_SEC_CONFIG, "5");
    props.put(BigQuerySinkConfig.GCS_BUCKET_NAME_CONFIG, bucketName);
    props.put(BigQuerySinkConfig.GCS_FOLDER_NAME_CONFIG, folderName);
    props.put(BigQuerySinkConfig.AUTO_CREATE_BUCKET_CONFIG, "false");
    props.put(BigQuerySinkConfig.ALLOW_NEW_BIGQUERY_FIELDS_CONFIG, Boolean.toString(allowNew));
    props.put(BigQuerySinkConfig.ALLOW_BIGQUERY_REQUIRED_FIELD_RELAXATION_CONFIG, Boolean.toString(allowRelax));
    props.put(BigQuerySinkConfig.GCS_LOAD_AUTODETECT_CONFIG, Boolean.toString(autodetect));
    return props;
  }

  private void produceRecord(Schema schema, Struct value) {
    byte[] payload = valueConverter.fromConnectData(topic, schema, value);
    connect.kafka().produce(topic, null, new String(payload, StandardCharsets.UTF_8));
  }

  private void waitForRows(int expected) throws InterruptedException {
    waitForCondition(() -> {
      try {
        Table table = bigQuery.getTable(TableId.of(dataset(), tableName));
        if (table == null) {
          return false;
        }
        TableResult tableResult = bigQuery.query(
            com.google.cloud.bigquery.QueryJobConfiguration.of(String.format(
                "SELECT COUNT(*) FROM `%s`.`%s`",
                dataset(),
                tableName))
        );
        if (!tableResult.iterateAll().iterator().hasNext()) {
          return false;
        }
        return tableResult.iterateAll().iterator().next().get(0).getLongValue() >= expected;
      } catch (Exception e) {
        return false;
      }
    }, LOAD_TIMEOUT_MS, "Timed out waiting for expected rows in BigQuery");
  }

  private String buildFolderName(String method) {
    String baseFolder = gcsFolder();
    String unique = String.format(Locale.ROOT, "%s-%d", method.toLowerCase(Locale.ROOT), System.nanoTime());
    if (baseFolder == null || baseFolder.isEmpty()) {
      return unique;
    }
    return baseFolder.endsWith("/") ? baseFolder + unique : baseFolder + "/" + unique;
  }
}
