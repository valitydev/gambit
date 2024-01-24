package dev.vality.gambit.service.impl;

import dev.vality.gambit.exception.FileProcessingException;
import dev.vality.gambit.model.DataEntries;
import dev.vality.gambit.service.FileService;
import dev.vality.gambit.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class CsvFileServiceImpl implements FileService {

    @Override
    public DataEntries process(BufferedReader bufferedReader) {
        return process(bufferedReader, null);
    }

    @Override
    public DataEntries process(BufferedReader bufferedReader, List<String> existingHeaders) {
        try {
            List<String> headers = getHeaders(bufferedReader, existingHeaders);
            Set<String> values = getValues(bufferedReader, headers);
            return new DataEntries(headers, values);
        } catch (IOException e) {
            log.error("Error during csv file processing.");
            throw new FileProcessingException(e);
        }
    }

    private List<String> getHeaders(BufferedReader bufferedReader, List<String> existingHeaders) throws IOException {
        String headerLine = bufferedReader.readLine();
        if (!StringUtils.hasText(headerLine)) {
            log.error("Empty file");
            throw new IllegalArgumentException();
        }
        List<String> inputHeaders = trimAndSplitLine(headerLine);
        if (!CollectionUtils.isEmpty(existingHeaders)) {
            validateByExistingHeaders(inputHeaders, existingHeaders);
        }
        return inputHeaders;
    }

    private Set<String> getValues(BufferedReader bf, List<String> headers) {
        Set<String> values = bf.lines()
                .map(line -> validateAndTrimValues(line, headers.size()))
                .collect(Collectors.toSet());
        if (CollectionUtils.isEmpty(values)) {
            log.error("No values in file");
            throw new IllegalArgumentException();
        }
        return values;
    }

    private String validateAndTrimValues(String line, int headersCount) {
        List<String> trimmedValues = trimAndSplitValueLine(line);
        if (headersCount != trimmedValues.size()) {
            log.error("Line '{}' doesn't match headers count {}", line, headersCount);
            throw new IllegalArgumentException();
        }
        return String.join(Constants.SEPARATOR, trimmedValues);
    }

    private void validateByExistingHeaders(List<String> inputHeaders, List<String> existingHeaders) {
        if (inputHeaders.size() != existingHeaders.size()) {
            log.error("Input headers {} don't match data set info headers {}", inputHeaders, existingHeaders);
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < inputHeaders.size(); i++) {
            if (!inputHeaders.get(i).equals(existingHeaders.get(i))) {
                log.error("Input headers {} don't match data set info headers {}", inputHeaders, existingHeaders);
                throw new IllegalArgumentException();
            }
        }
    }

    private List<String> trimAndSplitLine(String line) {
        return Arrays.stream(line.split(Constants.SEPARATOR))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private List<String> trimAndSplitValueLine(String line) {
        List<String> values = new ArrayList<>();
        int startPosition = 0;
        boolean isInsideQuotes = false;
        for (int currentPosition = 0; currentPosition < line.length(); currentPosition++) {
            if (line.charAt(currentPosition) == Constants.DOUBLE_QUOTES) {
                isInsideQuotes = !isInsideQuotes;
            } else if (line.charAt(currentPosition) == Constants.SEPARATOR.charAt(0) && !isInsideQuotes) {
                values.add(line.substring(startPosition, currentPosition).trim());
                startPosition = currentPosition + 1;
            }
        }
        String lastValue = line.substring(startPosition).trim();
        if (lastValue.equals(Constants.SEPARATOR)) {
            values.add("");
        } else {
            values.add(lastValue);
        }
        return values;
    }

}
