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
package vn.sps.serialization;

import java.io.IOException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializationSchema extends AbstractDeserializationSchema<JsonNode>{

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final long serialVersionUID = -549701789070382544L;
    
    private static final Logger LOGGER  = LoggerFactory.getLogger(JsonDeserializationSchema.class);

    @Override
    public JsonNode deserialize(byte[] message) throws IOException {
        
        if (message == null) {
            return null;
        }
        JsonNode data = null;
        try {
            data = objectMapper.readTree(message);
        } catch (Exception e) {
            LOGGER.error("Error while deserializing the message {}. So skip this message. Error detail: {}",
                new String(message), e);
        }
        return data;
    }
}