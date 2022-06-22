/*
 * Class: ReportSLA
 *
 * Created on May 24, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.ifa.study.flink.report;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.RecordTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import vn.sps.cdipp.AbstractDataAnalyzer;
import vn.sps.cdipp.factory.SourceFactory;

public class SLADataAnalyzer extends AbstractDataAnalyzer<JsonNode> {

	private static final Logger log = LoggerFactory.getLogger(SLADataAnalyzer.class);

	private static final long serialVersionUID = 4919141782930956120L;
	
	public SLADataAnalyzer(final String[] args) throws IOException {
		super(args);
	}

	@Override
	protected DataStream<JsonNode> analyze(final DataStream<JsonNode> dataStream) {
        /*
         * Create two data streams
         */
		final DataStream<JsonNode> dataStream1 = dataStream
				.assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.<JsonNode>create());
		final Properties properties = this.getConfiguration("userInfo");
		final DataStreamSource<Row> dataStream2 = SourceFactory.createS3Source(properties,
				getStreamExecutionEnvironment());
		
		/*
         * Convert data stream to table
         */
		final StreamTableEnvironment tableEnv = getStreamTableEnvironment();
		
		final Schema userInfoSchema = Schema.newBuilder()
				.column("f0", "STRING NOT NULL")
				.column("f1", "STRING NOT NULL")
				.column("f2", "STRING NOT NULL")
				.primaryKey("f0")
				.build();
		
		final Table userInfoTable = tableEnv.fromChangelogStream(dataStream2, userInfoSchema);
		tableEnv.createTemporaryView("userInfoTable", userInfoTable);

		final Schema kafkaSchema = Schema.newBuilder()
				.column("f0", "STRING")
				.build();
		
		final Table kafkaTable = tableEnv.fromDataStream(dataStream1, kafkaSchema);
		kafkaTable.execute();
//		tableEnv.createTemporaryView("kafkaTable", kafkaTable);
//		
//		getStreamTableEnvironment().executeSql("SELECT * FROM kafkaTable").print();
		
		/*
         * Enrich the user name json
         */
		
//		Table enrichedTable = getStreamTableEnvironment().sqlQuery("SELECT userInfo.f1 , kafka.f FROM userInfoTable userInfo JOIN kafkaTable kafka ON userInfo.f1 = kafka.f0");
		
//		dataStream1
//			.map(enrichUserNameJson())
//			.print("After joinning - ");
		
//		dataStream1
//			.join(dataStream2)
//			.where(getDataStream1Key("where"))
//			.equalTo(getDataStream1Key("equalTo"))
//			.window(TumblingEventTimeWindows.of(Time.seconds(5)))
//			.apply(new JoinFunction<JsonNode, JsonNode, String>() {
//	
//				private static final long serialVersionUID = -7013974409091960477L;
//	
//				@Override
//				public String join(JsonNode first, JsonNode second) throws Exception {
//					
//					System.out.println("########first: " + first);
//					System.out.println("###########second: " + second);
//					
//					return first+" "+second;
//				}
//			})
//			.print();
		return dataStream;
	}

	private MapFunction<JsonNode, JsonNode> enrichUserNameJson() {
		return new MapFunction<JsonNode, JsonNode>() {

			private static final long serialVersionUID = -7826288729532687462L;

			@Override
			public JsonNode map(JsonNode value) throws Exception {
				
				final String name = value.get("name").asText();
				
				final TableResult joinedUserInfoTableResult = getStreamTableEnvironment()
						.sqlQuery(String.format("SELECT * FROM userInfoTable WHERE f1 = '%s'", name))
						.execute();
				
				final CloseableIterator<Row> iter = joinedUserInfoTableResult.collect();
				
				String fullName = null;
				while (iter.hasNext()) {
					final Row row = (Row) iter.next();
					fullName = (String) row.getField("fullName");
				}
				
				final ObjectNode objectNode = (ObjectNode) value; 
				objectNode.put("fullName", fullName);
				objectNode.toPrettyString();
				
				return objectNode;
			}
		};
	}

	private CoProcessFunction<JsonNode, Row, String> lookupFullName() {
		return new CoProcessFunction<JsonNode, Row, String>() {

			private ValueState<Row> referenceDataState = null;
			
			private static final long serialVersionUID = -6905557599249605381L;

			@Override
			public void open(Configuration config) {
				
				System.out.println("Open function is running...!");
				
				ValueStateDescriptor<Row> cDescriptor = new ValueStateDescriptor<>(
						"referenceData",
						TypeInformation.of(Row.class)
				);
				referenceDataState = getRuntimeContext().getState(cDescriptor);
			}
			

			@Override
			public void processElement1(JsonNode jsonKafka, CoProcessFunction<JsonNode, Row, String>.Context arg1,
					Collector<String> output) throws Exception {
				System.out.println("get name: " + referenceDataState.value());
				output.collect(jsonKafka + " with " + referenceDataState.value());
			}

			@Override
			public void processElement2(Row row, CoProcessFunction<JsonNode, Row, String>.Context arg1,
					Collector<String> arg2) throws Exception {
				
				System.out.println("update name: " + row);
				referenceDataState.update(row);
			}

		};
	}

	private KeySelector<Row, String> getDataStream2Key() {
		return new KeySelector<Row, String>() {
					private static final long serialVersionUID = -8244502354779754470L;

					@Override
					public String getKey(Row value) throws Exception {
						System.out.println("connect:value " + value.getField(1));
						return (String) value.getField(1);
					}
				};
	}

	private KeySelector<JsonNode, String> getDataStream1Key(String functionName) {
		return new KeySelector<JsonNode, String>() {
			private static final long serialVersionUID = -8244502354779754470L;

			@Override
			public String getKey(JsonNode value) throws Exception {
				System.out.println(functionName + ":value " + value.get("name").asText());
				return value.get("name").asText();
			}
		};
	}

	private MapFunction<JsonNode, JsonNode> transformJsonNode(final ObjectMapper mapper) {
		return json -> {
			final ObjectNode mappedJson = mapper.createObjectNode();

			final DocumentContext jsonContext = JsonPath.parse(json.toString());

			final String traceId = jsonContext.read("$.traceId");
			mappedJson.put("traceId", traceId);
//            mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
//            mappedJson.put("status", jsonContext.<String>read("$.managementData.status"));
//            mappedJson.put("eventTime", jsonContext.<String>read("$.managementData.stepsMetadata[0].startTime"));
			log.info("Extract report record with traceId {}", traceId);

			return (JsonNode) mappedJson;
		};
	}
	
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

		private static final long serialVersionUID = 2099042171426356537L;

		private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return new RecordTimestampAssigner<T>();
        }
    }
}