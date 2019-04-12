package ru.ikss.jiratask.git;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * @author dalex, nenahov
 */
public class JsonMapper {

    public static final String DATETIME_MASK = "dd.MM.yyyy HH:mm:ss.SSS";
    private static Map<String, JsonMapper> instances = new ConcurrentHashMap<>();
    private ObjectMapper objectMapper;
    private boolean prettyPrint;

    public static JsonMapper getInstance(boolean prettyPrint) {
        return instances.computeIfAbsent("" + prettyPrint, k -> new JsonMapper(prettyPrint));
    }

    private JsonMapper(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat(DATETIME_MASK));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        objectMapper.setSerializationInclusion(Include.NON_NULL);
    }

    public <T> T readValue(String content, Class<T> aClass) throws IOException {
        return objectMapper.readValue(content, aClass);
    }

    public JsonNode readTree(String content) throws IOException {
        return objectMapper.readTree(content);
    }

    @SuppressWarnings("rawtypes")
    public <T> T readValue(String content, TypeReference valueTypeRef) throws IOException {
        return objectMapper.readValue(content, valueTypeRef);
    }

    public String writeValue(Object obj) throws IOException {
        return prettyPrint ? objectMapper.writer().withDefaultPrettyPrinter().writeValueAsString(obj) : objectMapper.writeValueAsString(obj);
    }
}
