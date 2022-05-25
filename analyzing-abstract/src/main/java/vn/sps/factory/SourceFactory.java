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

import org.apache.flink.connector.kafka.source.KafkaSource;

import com.fasterxml.jackson.databind.JsonNode;

import vn.sps.deserialization.JsonDeserializationSchema;

public final class SourceFactory {
    
    private SourceFactory() {
    }
    
    public static KafkaSource<JsonNode> createKafkaSource(Properties properties)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {

        final String topic = properties.getProperty("topic");
        final String deserializationValue = properties.getProperty("value.deserializer");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Class<?> loadedMyClass = classLoader.loadClass(deserializationValue);

        JsonDeserializationSchema deserializationSchema = (JsonDeserializationSchema) loadedMyClass.getConstructor().newInstance();

        return KafkaSource.<JsonNode> builder().setTopics(topic).setValueOnlyDeserializer(deserializationSchema).setProperties(properties).build();
    }
}