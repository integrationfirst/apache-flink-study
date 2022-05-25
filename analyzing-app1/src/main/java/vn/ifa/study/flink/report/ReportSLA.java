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

import org.apache.flink.streaming.api.datastream.DataStream;

import com.fasterxml.jackson.databind.JsonNode;

import vn.sps.AbstractDataStream;

public class ReportSLA extends AbstractDataStream<JsonNode>{

    protected ReportSLA(String[] args) throws IOException {
        super(args);
    }

    @Override
    protected void execute(DataStream<JsonNode> dataStream) {
                
    }
}
