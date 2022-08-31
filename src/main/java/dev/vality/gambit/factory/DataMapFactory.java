package dev.vality.gambit.factory;

import dev.vality.gambit.util.Constants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataMapFactory {

    public static Map<String, String> createDataMap(String headers, String values) {
        if (!StringUtils.hasLength(headers) || !StringUtils.hasLength(values)) {
            log.error("Headers or values cannot be empty. headers: {}, values {}", headers, values);
            throw new IllegalArgumentException();
        }
        String[] splitHeaders = headers.split(Constants.SEPARATOR);
        String[] splitValues = values.split(Constants.SEPARATOR);
        if (splitValues.length != splitHeaders.length) {
            log.error("Error during split. headers: {}, values {}", splitHeaders, splitValues);
            throw new IllegalArgumentException();
        }

        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < splitHeaders.length; i++) {
            result.put(splitHeaders[i], splitValues[i]);
        }
        return result;
    }

}
