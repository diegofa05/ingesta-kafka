/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dot.ingesta.kafka.run;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dot.ingesta.kafka.operations.ConfigConstans;
import com.dot.ingesta.kafka.pipeline.PipelineToBigquery;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 * 
 * Comando de ejecucion del dataflow
 * 
 * mvn compile exec:java \
      -Dexec.mainClass=com.dot.ingesta.kafka.run.StarterPipeline \
      -Dexec.args=" \
      --runner=DataflowRunner \
      --stagingLocation=gs://bucket-kafka-to-bigquery/temp/kafka/ \
      --inputTopic=fulfillment.dbo.Users \
      --bootstrapServers=34.75.142.243:9092
      --outputTable=servient-data-lake:kafka_to_bigquery.transactions"

 */
public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static void main(String[] args) {
		LOG.info("INICIANCO DATAFLOW KAFKA GCP");
		kafkaOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(kafkaOptions.class);
		
		/*
		 * Configurar opciones para del dataflow  
		 */
		options.setProject(ConfigConstans.PROYECT_ID);
		options.setJobName(ConfigConstans.JOB_NAME);
		options.setTempLocation(ConfigConstans.TEMP_LOCATION);
		options.setMaxNumWorkers(ConfigConstans.MAX_WORKERS);
		options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);
		options.setRegion(ConfigConstans.REGION);
		options.setNetwork(ConfigConstans.NETWORK);
		options.setSubnetwork(ConfigConstans.SUBNETWORK);
		options.setWorkerMachineType(ConfigConstans.MACHINE_TYPE);
		options.setDiskSizeGb(ConfigConstans.DISK_SIZE);
		options.setUsePublicIps(false);
		
		LOG.info("CREANDO PIPELINE");
		
		Pipeline pipeline = Pipeline.create(options);
		PipelineToBigquery.run(options, pipeline);
		pipeline.run();
	}
}
