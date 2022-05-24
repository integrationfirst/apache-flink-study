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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

abstract class AbstractDataStream {

    protected abstract <T> WatermarkStrategy<T> getWatermarkStrategy();

    protected abstract <T> Source<T, ?, ?> getSource();

    protected abstract String getSourceName();

    public void execute() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<?> stream = env.fromSource(getSource(), getWatermarkStrategy(), getSourceName());
        execute(stream);

        env.execute();
    }

    protected abstract void execute(DataStream<?> dataStream);
}