package dev.vality.gambit.service.impl;

import dev.vality.gambit.model.DataEntries;
import dev.vality.gambit.service.CsvService;
import dev.vality.gambit.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class CsvServiceImpl implements CsvService {

    private static final String FILE_TYPE = "text/csv";

    @Override
    public DataEntries process(MultipartFile file) {
        return process(file, null);
    }

    @Override
    public DataEntries process(MultipartFile file, List<String> existingHeaders) {
        validateFileType(file);
        try (BufferedReader bf = createBufferedReader(file)) {
            List<String> headers = getHeaders(bf, existingHeaders);
            List<String> values = getValues(bf, headers, file.getName());
            return new DataEntries(headers, values);
        } catch (IOException e) {
            log.error("Error during file processing. file: {}.", file.getName());
            throw new RuntimeException(e);
        }
    }

    private void validateFileType(MultipartFile file) {
        if (!FILE_TYPE.equals(file.getContentType())) {
            log.error("File {} has incorrect content type {}", file.getName(), file.getContentType());
            throw new IllegalArgumentException();
        }
    }

    private BufferedReader createBufferedReader(MultipartFile file) throws IOException {
        return new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
    }

    private List<String> getHeaders(BufferedReader bufferedReader, List<String> existingHeaders) throws IOException {
        String headerLine = bufferedReader.readLine();
        if (!StringUtils.hasText(headerLine)) {
            log.error("Empty file");
            throw new IllegalArgumentException();
        }
        List<String> inputHeaders = trimAndSplitLine(headerLine);
        if (!CollectionUtils.isEmpty(existingHeaders)) {
            validateExistingHeaders(inputHeaders, existingHeaders);
        }
        return inputHeaders;
    }

    private List<String> getValues(BufferedReader bf, List<String> headers, String fileName) {
        List<String> values = bf.lines()
                .map(line -> validateAndTrimValues(line, headers.size()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(values)) {
            log.error("No values for file: {}", fileName);
            throw new IllegalArgumentException();
        }
        return values;
    }

    private String validateAndTrimValues(String line, int headersCount) {
        List<String> trimmedValues = trimAndSplitLine(line);
        if (headersCount != trimmedValues.size()) {
            log.error("Line '{}' doesn't match headers count {}", line, headersCount);
            throw new IllegalArgumentException();
        }
        return String.join(Constants.SEPARATOR, trimmedValues);
    }

    private void validateExistingHeaders(List<String> inputHeaders, List<String> existingHeaders) {
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

}
