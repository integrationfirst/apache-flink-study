/*
 * Class: ReportSLA
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
package vn.ifa.study.flink.report;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.fasterxml.jackson.databind.JsonNode;

import vn.sps.AbstractDataAnalyzer;
import vn.sps.factory.SourceFactory;

public class SLADataAnalyzer extends AbstractDataAnalyzer<JsonNode> {

    public SLADataAnalyzer(String[] args) throws IOException {
        super(args);
    }

    @Override
    protected void analyze(DataStream<JsonNode> dataStream) {
        dataStream.map(json -> json);
        
        Properties userInfoProperties = this.getConfiguration("userInfo");
        Source source = SourceFactory.createS3Source(userInfoProperties);
    }
}
