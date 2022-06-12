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

import vn.ifa.study.flink.report.SLADataAnalyzer;

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
		final SLADataAnalyzer analyzer = new SLADataAnalyzer(args);
		analyzer.test();
	}

	/*
	 * private void test() throws Exception { StreamExecutionEnvironment env =
	 * StreamExecutionEnvironment.getExecutionEnvironment();
	 * 
	 * final DataStream<String> dataStream1 = env.fromElements("John", "Zbe",
	 * "Zico");
	 * 
	 * final DataStream<String> dataStream2 = env.fromElements("John", "Zbe",
	 * "Abe");
	 * 
	 * dataStream1 .join(dataStream2) .where(new KeySelector<String, String>() {
	 * 
	 * private static final long serialVersionUID = -8244502354779754470L;
	 * 
	 * @Override public String getKey(String value) throws Exception {
	 * System.out.println("where:json " + value); return value; } }) .equalTo(new
	 * KeySelector<String, String>() {
	 * 
	 * private static final long serialVersionUID = 6569463193999775400L;
	 * 
	 * @Override public String getKey(String value) throws Exception {
	 * System.out.println("equal:json " + value); return value; } })
	 * .window(TumblingEventTimeWindows.of(Time.seconds(5))) .apply(new
	 * JoinFunction<String, String, String>() {
	 * 
	 * private static final long serialVersionUID = -7013974409091960477L;
	 * 
	 * @Override public String join(String first, String second) throws Exception {
	 * 
	 * System.out.println("########first: " + first);
	 * System.out.println("###########second: " + second);
	 * 
	 * return first+" "+second; } }).print();
	 * 
	 * 
	 * env.execute(); }
	 */

}