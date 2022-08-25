package dev.vality.gambit.service.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.dao.DataSetInfoDao;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.service.DataSetInfoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataSetInfoServiceImpl implements DataSetInfoService {

    private final DataSetInfoDao dataSetInfoDao;

    @Override
    public Integer createDataSetInfo(DataSetInfo dataSetInfo) {
        log.debug("Creating dataSetInfo: {}", dataSetInfo);
        return dataSetInfoDao.save(dataSetInfo);
    }

    @Override
    public Map<Integer, DataSetInfo> getDataSetInfoByNames(List<String> dataSetInfoNames) throws DataSetNotFound {
        log.debug("Querying for dataSetInfo names: {}", dataSetInfoNames);
        return dataSetInfoDao.getByNames(new HashSet<>(dataSetInfoNames)).stream()
                .collect(Collectors.toMap(DataSetInfo::getId, dataSetInfo -> dataSetInfo));
    }

}
