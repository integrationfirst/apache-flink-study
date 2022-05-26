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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

import vn.sps.factory.SinkFactory;
import vn.sps.factory.SourceFactory;
import vn.sps.utils.SinkType;

public abstract class AbstractDataAnalyzer<IN> implements DataAnalyzer {

    private static final String SOURCE_GROUP = "source";

    private static final String SOURCE_NAME = "name";

    private static final String SINK_GROUP = "sink";
    
    private Map<String, Properties> configurations = new HashMap<>();

    protected AbstractDataAnalyzer(String[] args) throws IOException {
        configure(args);
    }

    private void configure(String[] args) throws IOException {
        loadKinesisProperties();
        overwriteByArguments(args);
    }

    private void overwriteByArguments(String[] args) {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final Map<String, String> localProperties = parameters.toMap();

        localProperties.keySet().forEach(fullKey -> {
            
            final String group = fullKey.substring(0, fullKey.indexOf("."));
            
            final String key = fullKey.substring(fullKey.indexOf(".") + 1);
            final Object value = localProperties.get(fullKey);
            
            if (this.configurations.containsKey(group)) {
                
                this.configurations.get(group).put(key, value);
            } else {
                final Properties properties = new Properties();
                properties.put(key, value);
                
                configurations.put(group, properties);
            }
        });
    }

    private void loadKinesisProperties() throws IOException {
        configurations.putAll(KinesisAnalyticsRuntime.getApplicationProperties());
    }

    protected Properties getConfiguration(String group){
        return configurations.get(group);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void analyze() throws Exception {

        final String sourceName = this.configurations.get(SOURCE_GROUP).getProperty(SOURCE_NAME);
        
        final String sinkType = this.configurations.get(SINK_GROUP).getProperty("type");
        
        final Source source = SourceFactory.createKafkaSource(this.configurations.get(SOURCE_GROUP));
        final SinkFunction sink = SinkFactory.createSink(SinkType.valueOf(sinkType),
            this.configurations.get(SINK_GROUP));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<IN> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName);

        analyze(stream);
        stream.addSink(sink);

        env.execute();
    }

    protected abstract void analyze(DataStream<IN> dataStream);
}