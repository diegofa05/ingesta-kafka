package com.dot.ingesta.kafka.pipeline;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dot.ingesta.kafka.operations.ConfigConstans;
import com.dot.ingesta.kafka.run.kafkaOptions;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PipelineToBigquery {

	private static final Logger LOG = LoggerFactory.getLogger(PipelineToBigquery.class);

	public static void run(kafkaOptions options, Pipeline pipeline) {
		LOG.info("Iniciando pipeline.... ");

		/*
		 * Step #1: Read messages in from Kafka
		 */
		PCollection<KV<String, String>> data = pipeline.apply("ReadFromKafka",
				KafkaIO.<String, String>read().withBootstrapServers(options.getBootstrapServers())
						.withTopic(options.getInputTopic()).withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializer(StringDeserializer.class).withoutMetadata());

		/**
		 * Step #2: Transform data to Bigquery
		 */
		PCollection<TableRow> dataRow = data.apply("TransformKafka", ParDo.of(new DoFn<KV<String, String>, TableRow>() {
			private static final long serialVersionUID = -3874432395528188784L;

			@ProcessElement
			public void processElement(ProcessContext c) {
				KV<String, String> message = c.element();
				TableRow tableRow = new TableRow();
				try {
					if (message.getValue() != null) {
						JsonObject jsonObject = new JsonParser().parse(message.getValue()).getAsJsonObject();
						JsonObject payload = jsonObject.get(ConfigConstans.PAYLOAD).getAsJsonObject();
						Long ts_ms = payload.get(ConfigConstans.TIMESTAMP).getAsLong();
						String operation = payload.get(ConfigConstans.OPERATION).getAsString();
						String transactionTime = dateFormater(ts_ms);
						LOG.info(String.format("Transaction Time: [%s] \n Operation : [%s] \n payload: %s",
								transactionTime, operation, payload));
						/**
						 * Create Table row
						 */
						tableRow.set(ConfigConstans.TRANSACTION_TIME, transactionTime);
						tableRow.set(ConfigConstans.OPERATION_TYPE,
								payload.get(ConfigConstans.OPERATION).getAsString());

						if (operation.equalsIgnoreCase(ConfigConstans.DELETE)) {
							JsonObject dataInsert = payload.get(ConfigConstans.DATA_BEFORE).getAsJsonObject();
							tableRow.set("ID", isNull_int(dataInsert.get("ID")));
						} else {
							JsonObject dataInsert = payload.get(ConfigConstans.DATA_AFTER).getAsJsonObject();
							
							tableRow.set("ID", isNull_int(dataInsert.get("ID")));
							
							tableRow.set("First_name", isNull_string(dataInsert.get("FirstName")));
							
							tableRow.set("Last_name", isNull_string(dataInsert.get("LastName")));
							
							tableRow.set("Email", isNull_string(dataInsert.get("LastName")));
						}
						c.output(tableRow);
					}else {
						JsonObject dataInsert = new JsonParser().parse(message.getKey()).getAsJsonObject()
								.get(ConfigConstans.PAYLOAD).getAsJsonObject();
						
						tableRow.set(ConfigConstans.TRANSACTION_TIME,
								new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
						tableRow.set(ConfigConstans.OPERATION_TYPE, "Error");
						tableRow.set(ConfigConstans.ERROR_MESSAGE, "Value is null");
						tableRow.set("ID", isNull_int(dataInsert.get("ID")));
					}
				} catch (Exception e) {
					String errorMesage = String.format("Error in message: key: %s \n value: %s \n Error: %s",
							message.getKey(), message.getValue(), e);
					LOG.error(errorMesage);

					tableRow.set(ConfigConstans.TRANSACTION_TIME,
							new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
					tableRow.set(ConfigConstans.OPERATION_TYPE, "Error");
					tableRow.set(ConfigConstans.ERROR_MESSAGE, errorMesage);
					tableRow.set("ID", 0);
					c.output(tableRow);
				}

			}
		}));

		/**
		 * Step #3: Insert data to Bigquery
		 */
		dataRow.apply("WriteSuccessfulRecords", BigQueryIO.writeTableRows().withSchema(createSchema())
				.withTimePartitioning(new TimePartitioning().setType("DAY"))
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(WriteDisposition.WRITE_APPEND).withExtendedErrorInfo()
				.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
				.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()).to(options.getOutputTable()));

	}

	private static TableSchema createSchema() {
		// To learn more about BigQuery schemas:
		// https://cloud.google.com/bigquery/docs/schemas
		/**
		 * Ejemplos para la creacion del esquema en Bigquery
		 *
		 * TableSchema schema = new TableSchema() .setFields( Arrays.asList( new
		 * TableFieldSchema().setName("string_field").setType("STRING").setMode("REQUIRED"),
		 * new
		 * TableFieldSchema().setName("int64_field").setType("INT64").setMode("NULLABLE"),
		 * new TableFieldSchema().setName("float64_field").setType("FLOAT64"), //
		 * default mode is "NULLABLE" new
		 * TableFieldSchema().setName("numeric_field").setType("NUMERIC"), new
		 * TableFieldSchema().setName("bool_field").setType("BOOL"), new
		 * TableFieldSchema().setName("bytes_field").setType("BYTES"), new
		 * TableFieldSchema().setName("date_field").setType("DATE"), new
		 * TableFieldSchema().setName("datetime_field").setType("DATETIME"), new
		 * TableFieldSchema().setName("time_field").setType("TIME"), new
		 * TableFieldSchema().setName("timestamp_field").setType("TIMESTAMP"), new
		 * TableFieldSchema().setName("geography_field").setType("GEOGRAPHY"), new
		 * TableFieldSchema().setName("array_field").setType("INT64").setMode("REPEATED").setDescription("Setting
		 * the mode to REPEATED makes this an ARRAY<INT64>."), new
		 * TableFieldSchema().setName("struct_field").setType("STRUCT")
		 * .setDescription("A STRUCT accepts a custom data class, the fields must match
		 * the custom class fields.") .setFields( Arrays.asList( new
		 * TableFieldSchema().setName("string_value").setType("STRING"), new
		 * TableFieldSchema().setName("int64_value").setType("INT64")) ) ));
		 */

		TableSchema schema = new TableSchema().setFields(Arrays.asList(
				new TableFieldSchema().setName(ConfigConstans.TRANSACTION_TIME).setType("TIMESTAMP"),
				new TableFieldSchema().setName(ConfigConstans.OPERATION_TYPE).setType("STRING").setMode("REQUIRED"),
				new TableFieldSchema().setName(ConfigConstans.ERROR_MESSAGE).setType("STRING").setMode("NULLABLE"),
				new TableFieldSchema().setName("ID").setType("INT64").setMode("REQUIRED"),
				new TableFieldSchema().setName("First_name").setType("STRING").setMode("NULLABLE"),
				new TableFieldSchema().setName("Last_name").setType("STRING").setMode("NULLABLE"),
				new TableFieldSchema().setName("Email").setType("STRING").setMode("NULLABLE")));
		return schema;
	}

	private static String dateFormater(Long timestamp) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timestamp);
		SimpleDateFormat formatterWithTimeZone = new SimpleDateFormat(ConfigConstans.FORMAT_DATE);
		formatterWithTimeZone.setTimeZone(TimeZone.getTimeZone(ConfigConstans.TIMEZONE));
		return formatterWithTimeZone.format(cal.getTime());
	}
	
	private static String isNull_string (JsonElement element) {
		return element.isJsonNull()?null:element.getAsString();
	}
	
	private static Integer isNull_int (JsonElement element) {
		return element.isJsonNull()?null:element.getAsInt();
	}
	
	private static Float isNull_float (JsonElement element) {
		return element.isJsonNull()?null:element.getAsFloat();
	}
	
	private static Boolean isNull_boolean (JsonElement element) {
		return element.isJsonNull()?null:element.getAsBoolean();
	}
	
	private static Long isNull_long (JsonElement element) {
		return element.isJsonNull()?null:element.getAsLong();
	}

}
