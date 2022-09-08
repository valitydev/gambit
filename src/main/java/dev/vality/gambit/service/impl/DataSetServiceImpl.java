package dev.vality.gambit.service.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.factory.DataFactory;
import dev.vality.gambit.model.DataEntries;
import dev.vality.gambit.service.FileService;
import dev.vality.gambit.service.DataService;
import dev.vality.gambit.service.DataSetInfoService;
import dev.vality.gambit.service.DataSetService;
import dev.vality.gambit.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataSetServiceImpl implements DataSetService {

    private final DataService dataService;

    private final DataSetInfoService dataSetInfoService;

    private final FileService fileService;

    @Transactional
    @Override
    public void createDataSet(String dataSetName, MultipartFile file) {
        validateDataSetNameRequest(dataSetName);
        DataEntries dataEntries = fileService.process(file);
        Integer dataSetInfoId = dataSetInfoService.createDataSetInfo(
                new DataSetInfo(null, dataSetName, String.join(Constants.SEPARATOR, dataEntries.getHeaders())));
        dataService.saveDataBatch(dataEntries.getValues().stream()
                .map(value -> DataFactory.create(dataSetInfoId, value))
                .collect(Collectors.toList()));
    }

    @Transactional
    @Override
    public void updateDataSet(String dataSetName, MultipartFile file) throws DataSetNotFound {
        DataSetInfo dataSetInfo = dataSetInfoService.getDataSetInfoByName(dataSetName)
                .orElseThrow(DataSetNotFound::new);
        List<String> existingHeaders = Arrays.asList(dataSetInfo.getHeaders().split(Constants.SEPARATOR));
        DataEntries dataEntries = fileService.process(file, existingHeaders);
        dataService.saveDataBatch(dataEntries.getValues().stream()
                .map(value -> DataFactory.create(dataSetInfo.getId(), value))
                .collect(Collectors.toList()));
    }

    private void validateDataSetNameRequest(String dataSetName) {
        if (!StringUtils.hasText(dataSetName)) {
            log.error("Invalid dataSetName `{}`", dataSetName);
            throw new IllegalArgumentException();
        }
        dataSetInfoService.getDataSetInfoByName(dataSetName)
                .ifPresent(throwDataSetInfoAlreadyExist());
    }

    private Consumer<DataSetInfo> throwDataSetInfoAlreadyExist() {
        return dataSetInfo -> {
            log.error("Data set info already exists. dataSetInfo:{}", dataSetInfo);
            throw new DataSetInfoAlreadyExistException(dataSetInfo.getName());
        };
    }

}
