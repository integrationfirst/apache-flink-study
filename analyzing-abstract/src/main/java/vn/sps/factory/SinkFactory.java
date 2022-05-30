/*
 * Class: SinkFactory
 *
 * Created on May 25, 2022
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
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.SerializationException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class SinkFactory {

    private SinkFactory() {
    }
    
    @SuppressWarnings("unchecked")
    public static <T> SinkFunction<T> createFirehoseSink(Properties sinkProperties) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String deliveryStream = sinkProperties.getProperty("deliveryStream");
        return new FlinkKinesisFirehoseProducer<>(deliveryStream, new KinesisFirehoseSerializationSchema() {
            @Override
            public ByteBuffer serialize(Object element) {
                byte[] bytes = new byte[0];
                try {
                    bytes = objectMapper.writeValueAsBytes(element);
                } catch (Exception e) {
                    throw new SerializationException("Error serializing JSON message", e);
                }
                return ByteBuffer.wrap(bytes);
            }
        }, sinkProperties);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> SinkFunction<T> createKafkaSink(Properties sinkProperties)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {

        final String topic = sinkProperties.getProperty("topic");
        final String serializationValueSchema = (String) sinkProperties.remove("value.serializer");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> loadedMyClass = classLoader.loadClass(serializationValueSchema);
        SerializationSchema serializationSchema = (SerializationSchema) loadedMyClass.getConstructor().newInstance();
        
        return new FlinkKafkaProducer<>(topic, serializationSchema, sinkProperties);
    }
}