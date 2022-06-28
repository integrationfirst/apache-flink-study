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

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import vn.ifa.study.flink.report.SLADataAnalyzer;
import vn.sps.cdipp.factory.SourceFactory;

public class ReportSLAAnalyticsApplication {

    public static void main(final String[] args) throws Exception {
		final SLADataAnalyzer analyzer = new SLADataAnalyzer(args);
		analyzer.analyze();
//        test();
    }

    private static void test() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        /*
         * Create kafka source table
         */
        TableResult tableResult = tableEnv.executeSql(
                String.join("\n", "CREATE TABLE kafkaTable (", "  eventId STRING,",
                        "  managementData ROW< stepsMetadata ARRAY< ROW<startTime STRING, endTime STRING> > >,",
                        "  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',", "  WATERMARK FOR ts AS ts", ") WITH (",
                        "  'connector' = 'kafka',", "  'topic' = 'analyzing-name-input-channel',",
                        "  'properties.bootstrap.servers' = '10.10.15.85:32092',",
//                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'format' = 'json'", ")"));

//		tableEnv.executeSql(String.join(
//		        "\n",
//                "CREATE TABLE firehoseTable ("
//                , "  startTime STRING,"
//                , "  endTime STRING,"
//                , "  eventID STRING"
//                , ") WITH ("
//                , "  'connector' = 'firehose',"
//                , "  'delivery-stream' = 'report-demo-data',"
//                , "  'aws.region' = 'eu-central-1',"
//                , "  'aws.credentials.basic.accesskeyid' = 'AKIATV2PQP5GPBZLMAVC',"
//                , "  'aws.credentials.basic.secretkey' = 'eMJEfz1gbg2zwmxwBRPyexA4JWeFYgnOvSSBeI,l',"
//                , "  'format' = 'json'"
//                , ")"));
//		tableEnv.from("kafkaTable").execute().print();

        /*
         * Create a s3 data stream
         */
        final Properties properties = new Properties();
        properties.put("filePath", "file:///C:/Users/tqminh_1/Downloads/test");
        properties.put("numberOfColumns", "3");
        properties.put("fileProcessingMode", "PROCESS_CONTINUOUSLY");
        properties.put("intervalCheck", "1000");

        final DataStreamSource<Row> s3DataStream = SourceFactory.createS3Source(properties, env);
        /*
         * Convert data stream to table
         */
        final Schema userInfoSchema = Schema.newBuilder()
                                            .column("f0", "STRING NOT NULL")
                                            .column("f1", "STRING NOT NULL")
                                            .column("f2", "STRING NOT NULL")
                                            .primaryKey("f0")
                                            .build();
        final Table userInfoTable = tableEnv.fromDataStream(s3DataStream, userInfoSchema);
        tableEnv.createTemporaryView("userInfoTable", userInfoTable);
        /*
         * Execute sql
         */
//		Table enrichedTable = tableEnv.sqlQuery("SELECT * "
//				+ "FROM userInfoTable userInfo JOIN kafkaTable kafka ON userInfo.f1 = kafka.name");

        Table enrichedTable = tableEnv.sqlQuery("SELECT managementData.stepsMetadata[1].startTime as startTime, "
                + "managementData.stepsMetadata[1].endTime as endTime " + "FROM kafkaTable");

//		enrichedTable.executeInsert("firehoseTable");

        final Properties kafkaproperties = new Properties();
        kafkaproperties.put("bootstrap.servers", "10.10.15.85:32092");
        kafkaproperties.put("topic", "analyzing-app-1-output-channel");

        DataStream<Row> dataStream = tableEnv.toDataStream(enrichedTable);
        String[] str = new String[] {"startTime"};
        TypeInformation[] informations = new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO};
        TypeInformation information = new RowTypeInfo(informations, str);

        FlinkKafkaProducer flinkKafkaProducer = new FlinkKafkaProducer<>("analyzing-app-1-output-channel",
                new JsonRowSerializationSchema.Builder(information).build(), kafkaproperties);

        dataStream.addSink(flinkKafkaProducer);

        dataStream.print();
        env.execute();
    }
}