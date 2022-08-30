package dev.vality.gambit.service.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.factory.DataFactory;
import dev.vality.gambit.service.DataService;
import dev.vality.gambit.service.DataSetInfoService;
import dev.vality.gambit.service.DataSetService;
import dev.vality.gambit.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataSetServiceImpl implements DataSetService {

    private final DataService dataService;

    private final DataSetInfoService dataSetInfoService;

    private static final String FILE_TYPE = "text/csv";

    @Transactional
    @Override
    public void createDataSet(String dataSetName, MultipartFile file) {
        dataSetInfoService.getDataSetInfoByName(dataSetName)
                .ifPresent(throwDataSetInfoAlreadyExist());
        validateFileType(file);
        try (BufferedReader bf = createBufferedReader(file)) {
            List<String> headers = getHeaders(bf);
            List<String> values = getValues(file, bf, headers);
            Integer dataSetInfoId = dataSetInfoService.createDataSetInfo(
                    new DataSetInfo(null, dataSetName, String.join(Constants.SEPARATOR, headers)));
            dataService.saveDataBatch(values.stream()
                    .map(value -> DataFactory.create(dataSetInfoId, value))
                    .collect(Collectors.toList())
            );
        } catch (IOException e) {
            log.error("Error during creation of data set. name: {}, file: {}.", dataSetName, file.getName());
            throw new RuntimeException(e);
        }
    }

    @Transactional
    @Override
    public void updateDataSet(String dataSetName, MultipartFile file) throws DataSetNotFound {
        DataSetInfo dataSetInfo = dataSetInfoService.getDataSetInfoByName(dataSetName)
                .orElseThrow(DataSetNotFound::new);
        validateFileType(file);

        try (BufferedReader bf = createBufferedReader(file)) {
            List<String> inputHeaders = getHeaders(bf);
            validateExistingHeaders(dataSetInfo, inputHeaders);
            Map<String, Data> data = getValues(file, bf, inputHeaders).stream()
                    .map(value -> DataFactory.create(dataSetInfo.getId(), value))
                    .collect(Collectors.toMap(Data::getValuesHash, entry -> entry));
            removeDuplicateData(dataSetInfo, data);
            dataService.saveDataBatch(new ArrayList<>(data.values()));
        } catch (IOException e) {
            log.error("Error during update of data set. name: {}, file: {}.", dataSetName, file.getName());
            throw new RuntimeException(e);
        }
    }

    private Consumer<DataSetInfo> throwDataSetInfoAlreadyExist() {
        return dataSetInfo -> {
            log.error("Data set info already exists. dataSetInfo:{}", dataSetInfo);
            throw new DataSetInfoAlreadyExistException();
        };
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

    private List<String> getHeaders(BufferedReader bufferedReader) throws IOException {
        String headerLine = bufferedReader.readLine();
        if (!StringUtils.hasText(headerLine)) {
            log.error("Empty file");
            throw new IllegalArgumentException();
        }
        return trimAndSplitLine(headerLine);
    }

    private List<String> getValues(MultipartFile file, BufferedReader bf, List<String> headers) {
        List<String> values = bf.lines()
                .map(line -> validateAndTrimValues(line, headers.size()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(values)) {
            log.error("No values for file: {}", file.getName());
            throw new IllegalArgumentException();
        }
        return values;
    }

    private String validateAndTrimValues(String line, int headersCount) {
        List<String> trimmedValues = trimAndSplitLine(line);
        if (headersCount != trimmedValues.size()) {
            log.error("Line {} doesn't match headers count {}", line, headersCount);
            throw new IllegalArgumentException();
        }
        return String.join(Constants.SEPARATOR, trimmedValues);
    }

    private void validateExistingHeaders(DataSetInfo dataSetInfo, List<String> inputHeaders) {
        String[] dataSetInfoHeaders = dataSetInfo.getHeaders().split(Constants.SEPARATOR);
        if (inputHeaders.size() != dataSetInfoHeaders.length) {
            log.error("Input headers {} don't match data set info headers {}", inputHeaders, dataSetInfoHeaders);
            throw new IllegalArgumentException();
        }
        for (int i = 0; i < inputHeaders.size(); i++) {
            if (!inputHeaders.get(i).equals(dataSetInfoHeaders[i])) {
                log.error("Input headers {} don't match data set info headers {}", inputHeaders, dataSetInfoHeaders);
                throw new IllegalArgumentException();
            }
        }
    }

    private void removeDuplicateData(DataSetInfo dataSetInfo, Map<String, Data> data) {
        Map<String, Data> alreadyExistingData =
                dataService.getByDataSetInfoAndValuesHashes(dataSetInfo.getId(), data.keySet());

        if (!CollectionUtils.isEmpty(alreadyExistingData)) {
            data.keySet().removeIf(key -> alreadyExistingData.containsKey(key)
                    && data.get(key).getValues().equals(alreadyExistingData.get(key).getValues()));
        }
    }

    private List<String> trimAndSplitLine(String line) {
        return Arrays.stream(line.split(Constants.SEPARATOR))
                .map(String::trim)
                .collect(Collectors.toList());
    }

}
