package com.dot.ingesta.kafka.run;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface kafkaOptions extends DataflowPipelineOptions {

	@Description("Kafka Bootstrap Servers")
    String getBootstrapServers();
    void setBootstrapServers(String value);

    @Description("Kafka topic to read the input from")
    String getInputTopic();
    void setInputTopic(String value);
    
    @Description("Bigquery table name ")
    String getOutputTable();
    void setOutputTable(String value);
}


