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
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import lombok.extern.slf4j.Slf4j;
import vn.sps.cdipp.AbstractDataAnalyzer;
import vn.sps.cdipp.factory.SourceFactory;
import vn.sps.cdipp.serialization.JsonNodeSerializationSchema;

@Slf4j
public class SLADataAnalyzer extends AbstractDataAnalyzer<JsonNode> {

	/**
	 *
	 */
	private static final long serialVersionUID = -6074032806582945913L;
	private final static Logger log = LoggerFactory.getLogger(SLADataAnalyzer.class);

	public SLADataAnalyzer(final String[] args) throws IOException {
		super(args);
	}

	public void test() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final Properties properties = this.getConfiguration("source");
		properties.put("deserializer", JsonNodeSerializationSchema.class);

		final KafkaSource<JsonNode> kafkaSource = SourceFactory.createKafkaSource(properties);

		final ObjectMapper mapper = new ObjectMapper();
		final JsonNode jsonNodeJohn = mapper.createObjectNode()
											.put("name", "John");
		final JsonNode jsonNodeZbe = mapper	.createObjectNode()
											.put("name", "Zbe");
		final JsonNode jsonNodeZico = mapper.createObjectNode()
											.put("name", "Zico");

		// final DataStream<JsonNode> dataStream1 = env.fromElements(jsonNodeJohn,
		// jsonNodeZbe, jsonNodeZico) df
		// .assignTimestampsAndWatermarks(waterMark());
		final DataStream<JsonNode> dataStream1 = env.fromSource(kafkaSource, this.waterMark(), "Kafka")
													.rebalance()
													.assignTimestampsAndWatermarks(this.waterMark());

		final DataStream<String> dataStream2 = env	.fromElements("John", "Zbe", "Abe")
													.assignTimestampsAndWatermarks(this.waterMark());

		dataStream1	.join(dataStream2)
					.where(new KeySelector<JsonNode, String>() {

						private static final long serialVersionUID = -8244502354779754470L;

						@Override
						public String getKey(final JsonNode value) throws Exception {
							System.out.println("where:value " + value);
							return value.get("name")
										.asText();
						}
					})
					.equalTo(new KeySelector<String, String>() {

						private static final long serialVersionUID = 6569463193999775400L;

						@Override
						public String getKey(final String value) throws Exception {
							System.out.println("equal:value " + value);
							return value;
						}
					})
					.window(SlidingEventTimeWindows.of(Time.minutes(50) /* size */, Time.minutes(10) /* slide */))
					.apply(new JoinFunction<JsonNode, String, String>() {

						private static final long serialVersionUID = -7013974409091960477L;

						@Override
						public String join(final JsonNode first, final String second) throws Exception {

							System.out.println("########first: " + first);
							System.out.println("###########second: " + second);

							return first + " " + second;
						}
					})
					.print();

		env.execute();
	}

	private <T> WatermarkStrategy<T> waterMark() {
		return new WatermarkStrategy<>() {

			/**
			 *
			 */
			private static final long serialVersionUID = -8696662739063680043L;

			@Override
			public WatermarkGenerator<T> createWatermarkGenerator(
					final org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}

			@Override
			public TimestampAssigner<T> createTimestampAssigner(final TimestampAssignerSupplier.Context context) {
				return (event, timestamp) -> System.currentTimeMillis();
			}

		};
	}

	@Override
	protected DataStream<JsonNode> analyze(final DataStream<JsonNode> dataStream) {
		final ObjectMapper mapper = new ObjectMapper();

		final Properties properties = new Properties();
		properties.putAll(this.getConfiguration("source"));
		properties.setProperty("topics", "analyzing-app-2-input-channel");
		KafkaSource<JsonNode> source = null;
		try {
			source = SourceFactory.createKafkaSource(properties);
		} catch (final InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		dataStream.assignTimestampsAndWatermarks(new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 8162042624195674743L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					final org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}

			@Override
			public TimestampAssigner<JsonNode> createTimestampAssigner(
					final TimestampAssignerSupplier.Context context) {
				return (event, timestamp) -> System.currentTimeMillis();
			}

		});
		final DataStream<JsonNode> dataStream2 = env.fromSource(source, new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					final org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}

			@Override
			public TimestampAssigner<JsonNode> createTimestampAssigner(
					final TimestampAssignerSupplier.Context context) {
				return (event, timestamp) -> System.currentTimeMillis();
			}

		}, "kafka2");
		dataStream2.assignTimestampsAndWatermarks(new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					final org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}

			@Override
			public TimestampAssigner<JsonNode> createTimestampAssigner(
					final TimestampAssignerSupplier.Context context) {
				return (event, timestamp) -> System.currentTimeMillis();
			}

		});
		return dataStream	.join(dataStream2)
							.where(new KeySelector<JsonNode, String>() {

								private static final long serialVersionUID = 1L;

								@Override
								public String getKey(final JsonNode value) throws Exception {
									System.out.println("where:json " + value);
									return value.get("traceId")
												.asText();
								}
							})
							.equalTo(new KeySelector<JsonNode, String>() {

								private static final long serialVersionUID = 8170863507711599110L;

								@Override
								public String getKey(final JsonNode value) throws Exception {
									System.out.println("equal:json " + value);
									return value.get("traceId")
												.asText();
								}
							})
							.window(TumblingEventTimeWindows.of(Time.seconds(50)))
							.apply(new JoinFunction<JsonNode, JsonNode, JsonNode>() {

								private static final long serialVersionUID = 8164167858060757109L;

								@Override
								public JsonNode join(final JsonNode first, final JsonNode second) throws Exception {

									System.out.println("########first: " + first);
									System.out.println("###########second: " + second);

									return first;
								}
							})
							.map(this.transformJsonNode(mapper));
	}

	private MapFunction<JsonNode, JsonNode> transformJsonNode(final ObjectMapper mapper) {
		return json -> {

			final ObjectNode mappedJson = mapper.createObjectNode();

			final DocumentContext jsonContext = JsonPath.parse(json.toString());

			final String traceId = jsonContext.<String>read("$.traceId");
			mappedJson.put("traceId", traceId);
			// mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
			// mappedJson.put("status",
			// jsonContext.<String>read("$.managementData.status"));
			// mappedJson.put("eventTime",
			// jsonContext.<String>read("$.managementData.stepsMetadata[0].startTime"));
			log.info("Extract report record with traceId {}", traceId);

			return (JsonNode) mappedJson;
		};
	}
}