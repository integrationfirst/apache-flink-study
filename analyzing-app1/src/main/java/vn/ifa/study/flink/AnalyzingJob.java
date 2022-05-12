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

package vn.ifa.study.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the tutorials and
 * examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class DataStreamJob {

    private static final String PROP_BOOTSTRAP_SERVERS = "bootstrap-servers";

    private static final String PROP_REQUEST_TOPIC = "request-topic";

    private static final String PROP_RESPONSE_TOPIC = "response-topic";

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        final String bootstrapServers = parameters.get(PROP_BOOTSTRAP_SERVERS);
        final String requestTopic = parameters.get(PROP_REQUEST_TOPIC);
        final String responseTopic = parameters.get(PROP_RESPONSE_TOPIC);

        KafkaSource<JsonNode> source = KafkaSource.<JsonNode> builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(requestTopic)
                .setGroupId("rd-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(JsonDeserializer.class))
                .build();

        KafkaRecordSerializationSchema<JsonNode> kafkaRecordSerializationSchema = KafkaRecordSerializationSchema.<JsonNode> builder()
                .setTopic(responseTopic)
                .setKafkaValueSerializer(JsonSerializer.class)
                .build();

        KafkaSink<JsonNode> sink = KafkaSink.<JsonNode> builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<JsonNode> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream.sinkTo(sink);
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like env.fromSequence(1,
         * 10);
         *
         * then, transform the resulting DataStream<Long> using operations like
         * .filter() .flatMap() .window() .process()
         *
         * and many more. Have a look at the programming guide:
         *
         * https://nightlies.apache.org/flink/flink-docs-stable/
         *
         */

        // Execute program, beginning computation.
        env.execute("Text Uppercase Processor");
    }
}
