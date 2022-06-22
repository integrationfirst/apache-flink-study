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

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import vn.sps.cdipp.factory.SourceFactory;

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
public class ReportSLAAnalyticsApplication {

	public static void main(final String[] args) throws Exception {
//		final SLADataAnalyzer analyzer = new SLADataAnalyzer(args);
//		analyzer.analyze();
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		/*
         * Create kafka source table
         */
		tableEnv.executeSql(
                String.join(
                        "\n",
                        "CREATE TABLE kafkaTable (",
                        "  name STRING,",
                        "  birthYear BIGINT,",
                        "  file ARRAY< ROW<startTime STRING, endTime STRING> >,",
                        "  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',",
                        "  WATERMARK FOR ts AS ts",
                        ") WITH (",
                        "  'connector' = 'kafka',",
                        "  'topic' = 'analyzing-name-input-channel',",
                        "  'properties.bootstrap.servers' = '10.10.15.85:32092',",
//                        "  'scan.startup.mode' = 'earliest-offset',",
                        "  'format' = 'json'",
                        ")"));
		tableEnv.from("kafkaTable").execute().print();
		/*
         * Create a s3 data stream
         */
		final Properties properties = new Properties();
		properties.put("filePath", "file:///C:/Users/tqminh_1/Downloads/test");
		properties.put("numberOfColumns", "3");
		properties.put("fileProcessingMode", "PROCESS_CONTINUOUSLY");
		properties.put("intervalCheck", "1000");
		
		final DataStreamSource<Row> s3DataStream = SourceFactory.createS3Source(properties, env);
		s3DataStream.print("s3-source--");
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
		Table enrichedTable = tableEnv.sqlQuery("SELECT * "
				+ "FROM userInfoTable userInfo JOIN kafkaTable kafka ON userInfo.f1 = kafka.name");
		enrichedTable.execute().print();
		env.execute();
	}
}