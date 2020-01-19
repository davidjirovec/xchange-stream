package info.bitrich.xchangestream.service.netty;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.jackson.datatype.VavrModule;

/**
 * This class should be merged with ObjectMapperHelper from XStream..
 *
 * @author Nikita Belenkiy on 19/06/2018.
 */
public class StreamingObjectMapperHelper {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new VavrModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    private StreamingObjectMapperHelper() {

    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }

}
