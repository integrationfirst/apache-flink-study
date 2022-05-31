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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import vn.sps.AbstractDataAnalyzer;
@Slf4j
public class SLADataAnalyzer extends AbstractDataAnalyzer<JsonNode> {

    public SLADataAnalyzer(String[] args) throws IOException {
        super(args);
    }

    @Override
    protected DataStream<JsonNode> analyze(DataStream<JsonNode> dataStream) {
        final ObjectMapper mapper = new ObjectMapper();
        
        return dataStream.map(transformJsonNode(mapper));
    }

    private MapFunction<JsonNode, JsonNode> transformJsonNode(final ObjectMapper mapper) {
        return json -> {

            final ObjectNode mappedJson = mapper.createObjectNode();

            DocumentContext jsonContext = JsonPath.parse(json.toString());

            final String traceId = jsonContext.<String>read("$.traceId");
            mappedJson.put("traceId", traceId);
            mappedJson.put("eventId", jsonContext.<String>read("$.eventId"));
            mappedJson.put("status", jsonContext.<String>read("$.managementData.status"));
            mappedJson.put("eventTime", jsonContext.<String>read("$.managementData.stepsMetadata[0].startTime"));
            log.info("Extract report record with traceId {}",traceId);

            return (JsonNode) mappedJson;
        };
    }
}
