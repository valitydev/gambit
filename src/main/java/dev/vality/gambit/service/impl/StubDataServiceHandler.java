package dev.vality.gambit.service.impl;

import dev.vality.gambit.*;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.exception.FileProcessingException;
import dev.vality.gambit.factory.BufferedReaderFactory;
import dev.vality.gambit.factory.DataLookupFactory;
import dev.vality.gambit.factory.DataMapFactory;
import dev.vality.gambit.service.DataLookupService;
import dev.vality.gambit.service.DataService;
import dev.vality.gambit.service.DataSetInfoService;
import dev.vality.gambit.service.DataSetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
@Service
public class StubDataServiceHandler implements StubDataServiceSrv.Iface {

    private final DataSetInfoService dataSetInfoService;

    private final DataLookupService dataLookupService;

    private final DataService dataService;

    private final DataSetService dataSetService;

    @Override
    public DataResponse getData(DataRequest dataRequest) throws TException {
        log.debug("Received getData request: {}", dataRequest);
        validateDataRequest(dataRequest);
        Map<Integer, DataSetInfo> dataSetInfos = getDataSetInfos(dataRequest);
        Set<Long> dataIds = getDataIds(dataRequest, dataSetInfos);
        Map<String, String> mergedDataMap = getDataMapByDataIds(dataSetInfos, dataIds);
        log.debug("getData result map: {}", mergedDataMap);
        return new DataResponse(mergedDataMap);
    }

    @Override
    public DataResponse getRandomDataRow(DataRowRequest dataRowRequest) throws TException {
        log.debug("Received getRandomDataRow request: {}", dataRowRequest);
        DataSetInfo dataSetInfo = dataSetInfoService.getDataSetInfoByName(dataRowRequest.getDataSetName())
                .orElseThrow(DataSetNotFound::new);
        Data randomDataRow = dataService.getRandomDataRow(dataSetInfo.getId());
        Map<String, String> mergedDataMap =
                DataMapFactory.createDataMap(dataSetInfo.getHeaders(), randomDataRow.getValues());
        log.debug("getRandomDataRow result map: {}", mergedDataMap);
        return new DataResponse(mergedDataMap);
    }

    @Override
    public void createDataSet(DataSetRequest dataSetRequest) {
        log.debug("received createDataSet request. dataSetName: {}, file type: {}",
                dataSetRequest.getDataSetName(), dataSetRequest.getFile().getSetField().getFieldName());
        validateDataSetRequest(dataSetRequest);
        try (BufferedReader bufferedReader = BufferedReaderFactory.create(dataSetRequest)) {
            dataSetService.createDataSet(dataSetRequest.getDataSetName(), bufferedReader);
        } catch (IOException e) {
            log.error("Error during createDataSet processing.");
            throw new FileProcessingException(e);
        }
        log.debug("created dataSet: {}", dataSetRequest.getDataSetName());
    }

    @Override
    public void updateDataSet(DataSetRequest dataSetRequest) throws TException {
        log.debug("received updateDataSet request. dataSetName: {}, file type: {}",
                dataSetRequest.getDataSetName(), dataSetRequest.getFile().getSetField().getFieldName());
        validateDataSetRequest(dataSetRequest);
        try (BufferedReader bufferedReader = BufferedReaderFactory.create(dataSetRequest)) {
            dataSetService.updateDataSet(dataSetRequest.getDataSetName(), bufferedReader);
        } catch (IOException e) {
            log.error("Error during updateDataSet processing.");
            throw new FileProcessingException(e);
        }
        log.debug("updated dataSetName: {}", dataSetRequest.getDataSetName());
    }

    private void validateDataRequest(DataRequest dataRequest) {
        if (CollectionUtils.isEmpty(dataRequest.getDataSetsNames())) {
            throw new IllegalStateException("Empty data set names list");
        }
    }

    private void validateDataSetRequest(DataSetRequest dataSetRequest) {
        if (!StringUtils.hasText(dataSetRequest.getDataSetName())
                || !dataSetRequest.getFile().isSetCsv() || dataSetRequest.getFile().getCsv().length == 0) {
            throw new IllegalStateException("Empty data set names list");
        }
    }

    private Map<Integer, DataSetInfo> getDataSetInfos(DataRequest dataRequest) throws DataSetNotFound {
        return dataSetInfoService.getDataSetInfoByNames(dataRequest.getDataSetsNames().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet())
        );
    }

    private Set<Long> getDataIds(DataRequest dataRequest, Map<Integer, DataSetInfo> dataSetInfos) {
        Map<Integer, Long> assignedDataIds =
                dataLookupService.getDataIds(dataSetInfos.keySet(), dataRequest.getLookupKey());
        Set<Long> newlyAssignedDataIds = assignDataIdsToNewHash(dataRequest, dataSetInfos, assignedDataIds);

        return Stream.concat(assignedDataIds.values().stream(), newlyAssignedDataIds.stream())
                .collect(Collectors.toSet());
    }

    private Set<Long> assignDataIdsToNewHash(DataRequest dataRequest,
                                             Map<Integer, DataSetInfo> dataSetInfos,
                                             Map<Integer, Long> assignedDataIds) {
        Set<Integer> unassignedDataSetInfoIds = getUnassignedDataSetInfoIds(dataSetInfos.keySet(), assignedDataIds);
        return createDataLookup(unassignedDataSetInfoIds, dataRequest.getLookupKey());
    }

    private Set<Integer> getUnassignedDataSetInfoIds(Set<Integer> dataSetInfoIds,
                                                     Map<Integer, Long> assignedDataIds) {
        if (CollectionUtils.isEmpty(assignedDataIds)) {
            return dataSetInfoIds;
        }
        return dataSetInfoIds.stream()
                .filter(dataSetId -> !assignedDataIds.containsKey(dataSetId))
                .collect(Collectors.toSet());
    }

    private Set<Long> createDataLookup(Set<Integer> dataSetInfoIds, int lookupKey) {
        Set<DataLookup> entities = dataSetInfoIds.stream()
                .map(dataSetInfoId ->
                        DataLookupFactory.create(dataSetInfoId, dataService.getRandomDataId(dataSetInfoId), lookupKey))
                .collect(Collectors.toSet());

        if (!CollectionUtils.isEmpty(entities)) {
            dataLookupService.saveBatch(entities);
            return entities.stream()
                    .map(DataLookup::getDataId)
                    .collect(Collectors.toSet());
        }

        return new HashSet<>();
    }

    private Map<String, String> getDataMapByDataIds(Map<Integer, DataSetInfo> dataSetInfos, Set<Long> dataIds) {
        return dataService.getDataByIds(dataIds).stream()
                .map(data -> DataMapFactory.createDataMap(
                        dataSetInfos.get(data.getDataSetInfoId()).getHeaders(),
                        data.getValues()
                ))
                .collect(HashMap::new, HashMap::putAll, HashMap::putAll);
    }

}
