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

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

abstract class AbstractDataStream {

    protected abstract <T> WatermarkStrategy<T> getWatermarkStrategy();

    protected abstract <T> Source<T, ?, ?> getSource();

    protected abstract String getSourceName();
    
    protected abstract Properties getSourceProperties();
    
    protected abstract Properties getSinkProperties();

    public void execute() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<?> stream = env.fromSource(getSource(), getWatermarkStrategy(), getSourceName());
        execute(stream);

        env.execute();
    }

    Properties extractProperties(
        final Map<String, Properties> applicationProperties,
        final Properties argsProperties,
        final String propertyGroup) {

        Properties properties = new Properties();
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
    
    protected abstract void execute(DataStream<?> dataStream);
}