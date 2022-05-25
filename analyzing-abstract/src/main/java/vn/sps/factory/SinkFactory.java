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

import java.util.Properties;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public final class SinkFactory {

    public static <T> SinkFunction<T> createFirehoseSink(Properties sinkProperties) {
        return null;
    }

}
