/*
 * Class: SourceFactory
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
package vn.sps.factory;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;

import com.fasterxml.jackson.databind.JsonNode;

public final class SourceFactory {
    
    private SourceFactory() {
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static KafkaSource<JsonNode> createKafkaSource(Properties properties)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
            InvocationTargetException, NoSuchMethodException {

        final String topic = properties.getProperty("topic");

        final String deserializationValue = properties.getProperty("value.deserializer");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> loadedMyClass = classLoader.loadClass(deserializationValue);
        DeserializationSchema deserializationSchema = (DeserializationSchema) loadedMyClass.getConstructor().newInstance();
        
        return KafkaSource.<JsonNode> builder().setTopics(topic).setValueOnlyDeserializer(deserializationSchema).setProperties(properties).build();
    }

    public static Source createS3Source(Properties userInfoProperties) {
        // TODO Auto-generated method stub
        
        return null;
    }
}