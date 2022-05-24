/*
 * Class: AbstractKafkaDataStream
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
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

public abstract class AbstractKafkaDataStream<T> extends AbstractS3Sink implements DataAnalyzer{

    private static final String CONSUMER_PROPERTIES_GROUP = "consumerProperties";

    private Properties argsProperties;
    
    private Map<String, Properties> applicationProperties;
    
    public AbstractKafkaDataStream(String[] args) throws IOException {
        this.argsProperties = ParameterTool.fromArgs(args).getProperties();
        this.applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
    }
    
    @Override
    protected Properties getSourceProperties() {
        return extractProperties(applicationProperties, argsProperties, CONSUMER_PROPERTIES_GROUP);
    }

    @Override
    protected <T> WatermarkStrategy<T> getWatermarkStrategy() {
        return null;
    }

    @Override
    protected String getSourceName() {
        return null;
    }

    @Override
    protected Source<T, ?, ?> getSource() {
        return KafkaSource.<T> builder()
                .setProperties(getSourceProperties())
                .build();
    }

    @Override
    public void analyze() throws Exception {
        execute();
    }
}