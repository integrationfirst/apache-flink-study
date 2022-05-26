/*
 * Class: JsonDeserializationSchema
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
package vn.sps.deserialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;

public class JsonDeserializationSchema extends AbstractDeserializationSchema<JsonNode>{

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final long serialVersionUID = -549701789070382544L;

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        
        if (message == null) {
            return null;
        }
        JsonNode data;
        try {
            data = objectMapper.readTree(message);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
        return data;
    }
}