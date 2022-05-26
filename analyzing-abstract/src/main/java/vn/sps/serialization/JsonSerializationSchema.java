/*
 * Class: JsonSerializationSchema
 *
 * Created on May 26, 2022
 *
 * (c) Copyright Swiss Post Solutions Ltd, unpublished work
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Swiss Post Solution.
 * Floor 4-5-8, ICT Tower, Quang Trung Software City
 */
package vn.sps.serialization;

import java.util.Collections;
import java.util.Set;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.common.errors.SerializationException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class JsonSerializationSchema implements SerializationSchema<JsonNode> {

    private static final long serialVersionUID = 3736768577785354164L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonSerializationSchema() {
        this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
    }

    /**
     * A constructor that additionally specifies some {@link SerializationFeature}
     * for the serializer
     *
     * @param serializationFeatures the specified serialization features
     * @param jsonNodeFactory the json node factory to use.
     */
    JsonSerializationSchema(final Set<SerializationFeature> serializationFeatures,
            final JsonNodeFactory jsonNodeFactory) {
        serializationFeatures.forEach(objectMapper::enable);
        objectMapper.setNodeFactory(jsonNodeFactory);
    }

    @Override
    public byte[] serialize(JsonNode data) {
        if (data == null)
            return null;
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }
}