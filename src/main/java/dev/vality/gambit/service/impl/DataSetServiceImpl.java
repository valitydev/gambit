package dev.vality.gambit.service.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.Data;
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
import org.springframework.util.CollectionUtils;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
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
        dataSetInfoService.getDataSetInfoByName(dataSetName)
                .ifPresent(throwDataSetInfoAlreadyExist());
        DataEntries dataEntries = fileService.process(file);
        Integer dataSetInfoId = dataSetInfoService.createDataSetInfo(
                new DataSetInfo(null, dataSetName, String.join(Constants.SEPARATOR, dataEntries.getHeaders())));
        dataService.saveDataBatch(dataEntries.getValues().stream()
                .map(value -> DataFactory.create(dataSetInfoId, value))
                .collect(Collectors.toList())
        );
    }

    @Transactional
    @Override
    public void updateDataSet(String dataSetName, MultipartFile file) throws DataSetNotFound {
        DataSetInfo dataSetInfo = dataSetInfoService.getDataSetInfoByName(dataSetName)
                .orElseThrow(DataSetNotFound::new);
        List<String> existingHeaders = Arrays.asList(dataSetInfo.getHeaders().split(Constants.SEPARATOR));
        DataEntries dataEntries = fileService.process(file, existingHeaders);
        Map<String, Data> data = dataEntries.getValues().stream()
                .map(value -> DataFactory.create(dataSetInfo.getId(), value))
                .collect(Collectors.toMap(Data::getValuesHash, entry -> entry));
        removeDuplicateData(dataSetInfo, data);
        dataService.saveDataBatch(new ArrayList<>(data.values()));
    }

    private Consumer<DataSetInfo> throwDataSetInfoAlreadyExist() {
        return dataSetInfo -> {
            log.error("Data set info already exists. dataSetInfo:{}", dataSetInfo);
            throw new DataSetInfoAlreadyExistException(dataSetInfo.getName());
        };
    }

    private void removeDuplicateData(DataSetInfo dataSetInfo, Map<String, Data> inputData) {
        Map<String, Data> alreadyExistingData =
                dataService.getByDataSetInfoAndValuesHashes(dataSetInfo.getId(), inputData.keySet());
        if (!CollectionUtils.isEmpty(alreadyExistingData)) {
            inputData.keySet().removeIf(isDuplicatedData(inputData, alreadyExistingData));
        }
    }

    private Predicate<String> isDuplicatedData(Map<String, Data> inputData, Map<String, Data> alreadyExistingData) {
        return key -> alreadyExistingData.containsKey(key)
                && inputData.get(key).getValues().equals(alreadyExistingData.get(key).getValues());
    }

}
