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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

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

        final ObjectMapper mapper = new ObjectMapper();
        
        Configuration conf = Configuration.builder().build();
        
        conf.setDefaults(new Configuration.Defaults() {

            private final JsonProvider jsonProvider = new JacksonJsonProvider();
            private final MappingProvider mappingProvider = new JacksonMappingProvider();
              
            @Override
            public JsonProvider jsonProvider() {
                return jsonProvider;
            }

            @Override
            public MappingProvider mappingProvider() {
                return mappingProvider;
            }
            
            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }
        });
        
        DataStream<JsonNode> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        stream.map(json -> {
            
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
        })
        .sinkTo(sink);
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
