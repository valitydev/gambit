package dev.vality.gambit.util;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class CsvUtils {

    public static final String SEPARATOR = ",";

    public static final char DOUBLE_QUOTES = '\"';

    public static List<String> trimAndSplitLine(String line) {
        return Arrays.stream(line.split(SEPARATOR))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public static List<String> trimAndSplitValueLine(String line) {
        List<String> values = new ArrayList<>();
        int startPosition = 0;
        boolean isInsideQuotes = false;
        for (int currentPosition = 0; currentPosition < line.length(); currentPosition++) {
            if (line.charAt(currentPosition) == DOUBLE_QUOTES) {
                isInsideQuotes = !isInsideQuotes;
            } else if (line.charAt(currentPosition) == SEPARATOR.charAt(0) && !isInsideQuotes) {
                values.add(line.substring(startPosition, currentPosition).trim());
                startPosition = currentPosition + 1;
            }
        }
        String lastValue = line.substring(startPosition).trim();
        if (lastValue.equals(SEPARATOR)) {
            values.add("");
        } else {
            values.add(lastValue);
        }
        return values;
    }
}
