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

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

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
    public static final String PROP_TOPIC = "topic";

    private static final ObjectMapper mapper = new ObjectMapper();
    public static final String CONSUMER_PROPERTIES = "consumerProperties";
    public static final String PRODUCER_PROPERTIES = "producerProperties";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String OUTPUT_PROPERTIES = "outputProperties";
    private static final String FIREHOSE_PROPERTIES = "firehoseProperties";

    public static void main(String[] args) throws Exception {

        final Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
         Properties consumerProperties = applicationProperties.get(CONSUMER_PROPERTIES);
         Properties producerProperties = applicationProperties.get(PRODUCER_PROPERTIES);
         Properties outputProperties = applicationProperties.get(OUTPUT_PROPERTIES);
        Properties firehoseProperties = applicationProperties.get(FIREHOSE_PROPERTIES);

        if (consumerProperties==null){
            consumerProperties=new Properties();
        }
        if (producerProperties==null){
            producerProperties=new Properties();
        }
        processArgs(args, consumerProperties,CONSUMER_PROPERTIES);
        processArgs(args,producerProperties,PRODUCER_PROPERTIES);

        final SimpleStringSchema deser = new SimpleStringSchema();

        final KafkaSource<String> source = KafkaSource.<String> builder().setProperties(consumerProperties)
                .setTopics(consumerProperties.getProperty(PROP_TOPIC))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(deser))
                .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String sink = outputProperties.getProperty("sink");
        if ("FIREHOSE".equalsIgnoreCase(sink)){
            final String streamDeliveryName = firehoseProperties.getProperty("streamDeliveryName");
            FlinkKinesisFirehoseProducer<String> firehoseProducer =
                    new FlinkKinesisFirehoseProducer<>(streamDeliveryName, deser, firehoseProperties);
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")
                    .addSink(firehoseProducer);
        }else {
            final FlinkKafkaProducer<String> kafkaProducer =
                    new FlinkKafkaProducer<String>(producerProperties.getProperty(PROP_TOPIC)
                    , deser,producerProperties);
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")
                    .addSink(kafkaProducer);
        }

        env.execute("Demo Processor");
    }

    private static void processArgs(String[] args, Properties props, String prefix) {
        if (args==null || args.length<=0){
            return;
        }
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String bt = parameters.get(PROP_BOOTSTRAP_SERVERS);
        props.setProperty(BOOTSTRAP_SERVERS,bt);

        switch (prefix){
            case CONSUMER_PROPERTIES:
                String rqt = parameters.get(PROP_REQUEST_TOPIC);
                props.setProperty(PROP_TOPIC,rqt);
                break;
            case PRODUCER_PROPERTIES:
                String rpt = parameters.get(PROP_RESPONSE_TOPIC);
                props.setProperty(PROP_TOPIC,rpt);
                break;
            default:
                break;
        }
    }



//    private static MapFunction<JsonNode, JsonNode> transfromMessage(final ObjectMapper mapper) {
//        return json -> {
//
//            final ObjectNode mappedJson = mapper.createObjectNode();
//
//            DocumentContext jsonContext = JsonPath.parse(json.toString());
//
//            mappedJson.set("fileUrls", mapper.createArrayNode());
//            mappedJson.put("traceId", jsonContext.<String>read("$.traceId"));
//            mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
//
//            List<Object> documentsAsJson = JsonPath.parse(json.toString()).read("$.documents");
//
//            for (Object documentAsJson : documentsAsJson) {
//
//                final JsonNode arrayNode = mappedJson.get("fileUrls");
//
//                List<Object> filesAsJson = JsonPath.parse(documentAsJson).read("$.files");
//
//                for (Object fileAsJson : filesAsJson) {
//
//                    final String fileUrl = JsonPath.parse(fileAsJson).read("$.fileUrl");
//
//                    ((ArrayNode) arrayNode).add(fileUrl);
//                }
//            }
//            return (JsonNode) mappedJson;
//        };
//    }
}
