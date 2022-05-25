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

import java.util.Properties;

import org.apache.flink.connector.kafka.source.KafkaSource;

public final class SourceFactory {
    
    public static <T> KafkaSource<T> createKafkaSource(Properties properties) {
        return KafkaSource.<T> builder().setProperties(properties).build();
    }
    
}