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

	private final static Logger log = LoggerFactory.getLogger(SLADataAnalyzer.class);
	
    public SLADataAnalyzer(String[] args) throws IOException {
        super(args);
    }
    
    public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties properties = getConfiguration("source");
		properties.put("deserializer", JsonNodeSerializationSchema.class);
		
		KafkaSource<JsonNode> kafkaSource = SourceFactory.createKafkaSource(properties);
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode jsonNodeJohn = mapper.createObjectNode().put("name", "John");
		JsonNode jsonNodeZbe = mapper.createObjectNode().put("name", "Zbe");
		JsonNode jsonNodeZico = mapper.createObjectNode().put("name", "Zico");
		
//		final DataStream<JsonNode> dataStream1 = env.fromElements(jsonNodeJohn, jsonNodeZbe, jsonNodeZico)
//				.assignTimestampsAndWatermarks(waterMark());
		final DataStream<JsonNode> dataStream1 = env.fromSource(kafkaSource, waterMark(), "Kafka").rebalance()
				.assignTimestampsAndWatermarks(waterMark());
		
        DataStream<String> dataStream2 = env.fromElements("John", "Zbe", "Abe")
        		.assignTimestampsAndWatermarks(waterMark());
        
        dataStream1
	        .join(dataStream2)
	        .where(new KeySelector<JsonNode, String>() {
	
				private static final long serialVersionUID = -8244502354779754470L;
	
				@Override
				public String getKey(JsonNode value) throws Exception {
					System.out.println("where:value " + value);
					return value.get("name").asText();
				}
			})
	        .equalTo(new KeySelector<String, String>() {
				
				private static final long serialVersionUID = 6569463193999775400L;
	
				@Override
				public String getKey(String value) throws Exception {
					System.out.println("equal:value " + value);
					return value;
				}
			})
	        .window(SlidingEventTimeWindows.of(Time.minutes(50) /* size */, Time.minutes(10) /* slide */))
	        .apply(new JoinFunction<JsonNode, String, String>() {
	
				private static final long serialVersionUID = -7013974409091960477L;
	
				@Override
				public String join(JsonNode first, String second) throws Exception {
					
					System.out.println("########first: " + first);
					System.out.println("###########second: " + second);
					
					return first+" "+second;
				}
			}).print();
	        
	        
	        env.execute();
	}

	private <T>  WatermarkStrategy<T> waterMark() {
		return new WatermarkStrategy<T>() {

			@Override
			public WatermarkGenerator<T> createWatermarkGenerator(
					org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}
			
			@Override
            public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (event, timestamp) -> System.currentTimeMillis();
            }
			
		};
	}

    @Override
    protected DataStream<JsonNode> analyze(DataStream<JsonNode> dataStream) {
        final ObjectMapper mapper = new ObjectMapper();
        
        final Properties properties = new Properties();
        properties.putAll(this.getConfiguration("source"));
        properties.setProperty("topics", "analyzing-app-2-input-channel");
        KafkaSource<JsonNode> source = null;
        try {
			source = SourceFactory.createKafkaSource(properties);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
        
        final StreamExecutionEnvironment env = this.env;
        
        dataStream.assignTimestampsAndWatermarks(new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 8162042624195674743L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}
			
			@Override
            public TimestampAssigner<JsonNode> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (event, timestamp) -> System.currentTimeMillis();
            }
			
		});
        final DataStream<JsonNode> dataStream2 = env.fromSource(source, new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}
			
			@Override
            public TimestampAssigner<JsonNode> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (event, timestamp) -> System.currentTimeMillis();
            }
			
		}, "kafka2");
        dataStream2.assignTimestampsAndWatermarks(new WatermarkStrategy<JsonNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public WatermarkGenerator<JsonNode> createWatermarkGenerator(
					org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new AscendingTimestampsWatermarks<>();
			}
			
			@Override
            public TimestampAssigner<JsonNode> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (event, timestamp) -> System.currentTimeMillis();
            }
			
		});
        return dataStream.join(dataStream2).where(new KeySelector<JsonNode, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(JsonNode value) throws Exception {
				System.out.println("where:json " + value);
				return value.get("traceId").asText();
			}
		}).equalTo(new KeySelector<JsonNode, String>() {
			
			private static final long serialVersionUID = 8170863507711599110L;

			@Override
			public String getKey(JsonNode value) throws Exception {
				System.out.println("equal:json " + value);
				return value.get("traceId").asText();
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(50)))
        .apply(new JoinFunction<JsonNode, JsonNode, JsonNode>() {

			private static final long serialVersionUID = 8164167858060757109L;

			@Override
			public JsonNode join(JsonNode first, JsonNode second) throws Exception {
				
				System.out.println("########first: " + first);
				System.out.println("###########second: " + second);
				
				return first;
			}
		})
        .map(transformJsonNode(mapper));
    }

    private MapFunction<JsonNode, JsonNode> transformJsonNode(final ObjectMapper mapper) {
        return json -> {

            final ObjectNode mappedJson = mapper.createObjectNode();

            DocumentContext jsonContext = JsonPath.parse(json.toString());

            final String traceId = jsonContext.<String>read("$.traceId");
            mappedJson.put("traceId", traceId);
//            mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
//            mappedJson.put("status", jsonContext.<String>read("$.managementData.status"));
//            mappedJson.put("eventTime", jsonContext.<String>read("$.managementData.stepsMetadata[0].startTime"));
            log.info("Extract report record with traceId {}",traceId);

            return (JsonNode) mappedJson;
        };
    }
}