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
import java.util.Optional;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;

public final class SinkFactory {

    private static final String DEFAULT_FIRE_HOSE_SERIALIZATION = "vn.sps.serialization.JsonKinesisFirehoseSerializationSchema";

    private static final String DEFAULT_KAFKA_SERIALIZATION = "vn.sps.serialization.JsonSerializationSchema";
    
    private SinkFactory() {
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> SinkFunction<T> createFirehoseSink(Properties sinkProperties) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        final String deliveryStream = sinkProperties.getProperty("deliveryStream");
        final String serializationValueSchema = sinkProperties.getProperty("serializer", DEFAULT_FIRE_HOSE_SERIALIZATION);

        final SerializationSchema serializationSchema = loadSerializationSchema(serializationValueSchema);

        return new FlinkKinesisFirehoseProducer<>(deliveryStream, serializationSchema, sinkProperties);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> SinkFunction<T> createKafkaSink(Properties sinkProperties)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException {

        final String topic = sinkProperties.getProperty("topic");
        String serializationValueSchema = Optional.<String> of(
            (String) sinkProperties.remove("value.serializer")).orElse(DEFAULT_KAFKA_SERIALIZATION);

        SerializationSchema serializationSchema = loadSerializationSchema(serializationValueSchema);
        
        return new FlinkKafkaProducer<>(topic, serializationSchema, sinkProperties);
    }

    @SuppressWarnings("rawtypes")
    private static SerializationSchema loadSerializationSchema(final String serializationValueSchema)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> loadedMyClass = classLoader.loadClass(serializationValueSchema);
        SerializationSchema serializationSchema = (SerializationSchema) loadedMyClass.getConstructor().newInstance();
        return serializationSchema;
    }
}