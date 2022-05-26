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

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vn.sps.factory.SinkFactory;
import vn.sps.factory.SourceFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public abstract class AbstractDataAnalyzer<IN> implements DataAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDataAnalyzer.class);
    
    private static final String SOURCE_GROUP = "source";

    private static final String SOURCE_NAME = "name";

    private static final String SINK_GROUP = "sink";
    
    private Properties sourceProperties;
    
    private Properties sinkProperties;
    
    protected AbstractDataAnalyzer(String[] args) throws IOException {
        configure(args);
    }

    private Map<String, Properties> configurations = new HashMap<>();

    private void configure(String[] args) throws IOException {
        loadKinesisProperties();
        overwriteByArguments(args);
    }

    private void overwriteByArguments(String[] args) throws IOException {
        final ParameterTool parameters = ParameterTool.fromArgs(args);parameters.tom
        final Properties localProperties = parameters.getProperties();

        final Map<String, Properties> kinesisProperties = KinesisAnalyticsRuntime.getApplicationProperties();

        this.sourceProperties = kinesisProperties.get(SOURCE_GROUP);
        this.sinkProperties = kinesisProperties.get(SINK_GROUP);

        if (Objects.isNull(sourceProperties) || Objects.isNull(sinkProperties)) {

            if(localProperties.isEmpty()) {
                throw new IllegalArgumentException("The program argments can not empty");
            }
            this.sourceProperties = new Properties();
            this.sinkProperties = new Properties();
            localProperties.keySet().forEach(element -> {

                String fullKey = (String) element;
                if (fullKey.length() > 1) {
                    final String group = fullKey.substring(0, fullKey.indexOf("."));
                    extractKeys(localProperties, fullKey, group, SOURCE_GROUP);
                    extractKeys(localProperties, fullKey, group, SINK_GROUP);
                }
            });
        }
    }

    private void loadKinesisProperties() throws IOException {
        configurations.putAll(KinesisAnalyticsRuntime.getApplicationProperties());
    }

    protected Properties getConfiguration(String group){
        return configurations.get(group);
    }

    @Override
    public void analyze() throws Exception {

        final String sourceName = (String) readAndWarningMandatoryProperty(sourceProperties, SOURCE_NAME, 1);
        final Source source = SourceFactory.createKafkaSource(sourceProperties);
        final SinkFunction sink = SinkFactory.createFirehoseSink(sinkProperties);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<IN> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), sourceName);

        analyze(stream);
        stream.addSink(sink);

        env.execute();
    }

    private Object readAndWarningMandatoryProperty(Properties properties, String key, Object defaultValue) {

        // WARN: Reimplement this function

        if (!properties.contains(key)) {
            LOGGER.warn("Cannot find mandatory configuration property [{}]. Use the default value {}", key, defaultValue);
        }
        return properties.getOrDefault(key,defaultValue);
    }
    
    private void extractKeys(final Properties localProperties, String fullKey, final String group, final String defaultGroupName) {
        if (group.equals(defaultGroupName)) {
            String key = fullKey.substring(fullKey.indexOf(".") + 1);
            Object value = localProperties.get(fullKey);
            this.sourceProperties.put(key, value);
        }
    }

    protected abstract void analyze(DataStream<IN> dataStream);
}