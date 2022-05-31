/*
 * Class: JsonKinesisFirehoseSerializationSchema
 *
 * Created on May 31, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.sps.serialization;

import java.nio.ByteBuffer;

import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.SerializationException;
import com.amazonaws.services.kinesisanalytics.flink.connectors.serialization.KinesisFirehoseSerializationSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonKinesisFirehoseSerializationSchema implements KinesisFirehoseSerializationSchema<JsonNode> {
    
    private static final long serialVersionUID = -120516862525505881L;
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ByteBuffer serialize(JsonNode element) {
        byte[] bytes = new byte[0];
        try {
            bytes = objectMapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
        return ByteBuffer.wrap(bytes);
    }
}