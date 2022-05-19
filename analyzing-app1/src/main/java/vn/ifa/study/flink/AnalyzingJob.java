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

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

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
public class AnalyzingJob {

    private static final String PROP_BOOTSTRAP_SERVERS = "bootstrap-servers";

    private static final String PROP_REQUEST_TOPIC = "request-topic";

    private static final String PROP_RESPONSE_TOPIC = "response-topic";

    private static final String PROP_NAME = "consumerConfigProperties";

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bootstrapServers = parameters.get(PROP_BOOTSTRAP_SERVERS);
        String requestTopic = parameters.get(PROP_REQUEST_TOPIC);
        String responseTopic = parameters.get(PROP_RESPONSE_TOPIC);
        
        final Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        final String mskBootstrapServers = (String) applicationProperties.get(PROP_NAME).get(
            "bootstrapServers");
        final String mskRequestTopic = (String) applicationProperties.get(PROP_NAME).get(
            "requestTopic");
        final String mskResponseTopic = (String) applicationProperties.get(PROP_NAME).get(
            "responseTopic");

        bootstrapServers = mskBootstrapServers != null ? mskBootstrapServers : bootstrapServers;
        requestTopic = mskRequestTopic != null ? mskRequestTopic : requestTopic;
        responseTopic = mskResponseTopic != null ? mskResponseTopic : responseTopic;
        
        KafkaSource<JsonNode> source = KafkaSource.<JsonNode> builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(requestTopic)
                .setGroupId("rd-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(JsonDeserializer.class))
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ObjectMapper mapper = new ObjectMapper();

        DataStream<JsonNode> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        stream.map(transfromMessage(mapper))
        .print();

        // Execute program, beginning computation.
        env.execute("Processor");
    }

    private static MapFunction<JsonNode, JsonNode> transfromMessage(final ObjectMapper mapper) {
        return json -> {
            
            final ObjectNode mappedJson = mapper.createObjectNode();

            DocumentContext jsonContext = JsonPath.parse(json.toString());
            
            mappedJson.set("fileUrls", mapper.createArrayNode());
            mappedJson.put("traceId", jsonContext.<String>read("$.traceId"));
            mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
            
            List<Object> documentsAsJson = JsonPath.parse(json.toString()).read("$.documents");

            for (Object documentAsJson : documentsAsJson) {

                final JsonNode arrayNode = mappedJson.get("fileUrls");
                
                List<Object> filesAsJson = JsonPath.parse(documentAsJson).read("$.files");
                
                for (Object fileAsJson : filesAsJson) {

                    final String fileUrl = JsonPath.parse(fileAsJson).read("$.fileUrl");

                    ((ArrayNode) arrayNode).add(fileUrl);
                }
            }
            return (JsonNode) mappedJson;
        };
    }
}
