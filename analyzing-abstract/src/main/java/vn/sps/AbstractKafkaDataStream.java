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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

public abstract class AbstractKafkaDataStream extends AbstractDataStream{

    private static final String CONSUMER_PROPERTIES = "consumerProperties";

    private static final String PRODUCER_PROPERTIES = "producerProperties";
    
    private Properties consumerProperties;
    
    private Properties producerProperties;
    
    public AbstractKafkaDataStream(String[] args) throws IOException {
        
        final Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final Properties argsProperties = parameters.getProperties();
        
        consumerProperties = extractProperties(applicationProperties, argsProperties, CONSUMER_PROPERTIES);
    }

    protected abstract <T> KafkaRecordDeserializationSchema<T> deserializationSchema();

    @Override
    protected <T> Source<T, ?, ?> getSource() {
        return KafkaSource.<T> builder()
                .setDeserializer(deserializationSchema())
                .setProperties(consumerProperties)
                .build();
    }
}