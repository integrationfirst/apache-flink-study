/*
 * Class: AbstractGeneralSink
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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

abstract class AbstractS3Sink extends AbstractDataStream{

    protected Properties argsProperties;
    
    protected Map<String, Properties> applicationProperties;
    
    public AbstractS3Sink(String[] args) throws IOException {
        this.argsProperties = ParameterTool.fromArgs(args).getProperties();
        this.applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
    }
    
    @Override
    protected Properties getSinkProperties() {
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected SinkFunction getSink() {
        return null;
    }
}