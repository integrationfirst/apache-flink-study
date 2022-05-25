/*
 * Class: StreamingData
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
package vn.sps;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import vn.sps.factory.SinkFactory;
import vn.sps.factory.SourceFactory;

abstract class AbstractDataStream<IN> {

    private static final String SOURCE_GROUP = "source";

    private static final String SINK_GROUP = "sink";
    
    @SuppressWarnings("rawtypes")
    protected abstract WatermarkStrategy getWatermarkStrategy();

    protected abstract String getSourceName();
    
    private Properties sourceProperties;
    
    private Properties sinkProperties;
    
    public AbstractDataStream(String[] args) throws IOException {
        
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final Properties localProperties = parameters.getProperties();
        
        final Map<String, Properties> kinesisProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        
        this.sourceProperties = kinesisProperties.get(SOURCE_GROUP);
        this.sinkProperties = kinesisProperties.get(SINK_GROUP);

        if(Objects.isNull(sourceProperties) || Objects.isNull(sinkProperties)) {
            // use local properties
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void execute() throws Exception {
        
        final Source<IN, ?, ?> source = SourceFactory.createKafkaSource(sourceProperties);
        
        final SinkFunction<IN> sink = SinkFactory.createFirehoseSink(sinkProperties);
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<IN> stream = env.fromSource(source, getWatermarkStrategy(), getSourceName());

        final DataStream<IN> dataStream = execute(stream);
        dataStream.addSink(sink);

        env.execute();
    }

    Properties extractProperties(
        final Map<String, Properties> applicationProperties,
        final Properties argsProperties,
        final String propertyGroup) {

        final Properties properties = new Properties();
        
        if (applicationProperties.get(propertyGroup) != null) {
            properties.putAll(applicationProperties.get(propertyGroup));
        } else {
            for (String configName : ConsumerConfig.configNames()) {
                final StringBuilder builder = new StringBuilder();
                final String fullConfigName = builder.append(propertyGroup).append(configName).toString();

                if (argsProperties.contains(fullConfigName)) {
                    properties.put(configName, argsProperties.get(fullConfigName));
                }
            }
        }
        return properties;
    }

    protected abstract DataStream<IN> execute(DataStream<IN> dataStream);
}