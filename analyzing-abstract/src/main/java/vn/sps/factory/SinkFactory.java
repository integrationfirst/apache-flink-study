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
import java.util.Properties;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer;
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.JsonSerializationSchema;

public final class SinkFactory {

    private SinkFactory() {
    }
    
    public static <T> SinkFunction<T> createFirehoseSink(Properties sinkProperties) {

        final String deliveryStream = sinkProperties.getProperty("deliveryStream");
        return new FlinkKinesisFirehoseProducer<>(deliveryStream, new JsonSerializationSchema<>(), sinkProperties);
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