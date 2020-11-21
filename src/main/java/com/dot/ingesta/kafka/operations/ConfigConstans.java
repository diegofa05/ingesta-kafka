package com.dot.ingesta.kafka.operations;

public interface ConfigConstans {

	/**
	 * Constants for configuration flow
	 */

	public static String PROYECT_ID = "servient-data-lake";
	public static String JOB_NAME = "kafka-to-bq-users";
	public static String TEMP_LOCATION = "gs://bucket-kafka-to-bigquery/temp/kafka/";
	public static Integer MAX_WORKERS = 100;
	public static String REGION = "us-east1";
	public static String NETWORK = "network-datalake";
	public static String SUBNETWORK = "regions/us-east1/subnetworks/subnet-a-datalake";
	public static String MACHINE_TYPE = "n1-standard-2";
	public static Integer DISK_SIZE = 100;

	public static String TIMEZONE = "America/Bogota";
	public static String FORMAT_DATE = "yyyy-MM-dd HH:mm:ss";

	/**
	 * Properties for Json's Kafka
	 */
	public static String PAYLOAD = "payload";
	public static String DATA_AFTER = "after";
	public static String DATA_BEFORE = "before";
	public static String TIMESTAMP = "ts_ms";

	public static String OPERATION = "op";

	public static String UPDATE = "u";
	public static String INSERT = "c";
	public static String DELETE = "d";
	
	
	/**
	 * Table name 
	 */
	public static String TRANSACTION_TIME  = "transaction_time";
	public static String OPERATION_TYPE = "operation_type";
	public static String ERROR_MESSAGE = "error_message";

}
