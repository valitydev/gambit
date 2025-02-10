package dev.vality.gambit.service.impl;

import dev.vality.gambit.dao.DataDao;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.service.DataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataServiceImpl implements DataService {

    private final DataDao dataDao;

    @Override
    public void saveDataBatch(List<Data> batch) {
        log.debug("Saving data batch. size: {}", batch.size());
        dataDao.saveBatch(batch);
    }

    @Override
    public Set<Data> getDataByIds(Set<Long> ids) {
        log.debug("Querying data ids: {}", ids);
        return new HashSet<>(dataDao.getDataByIds(ids));
    }

    @Override
    public Set<Data> getDataByDataSetInfoId(Integer id) {
        log.debug("Querying data by data set info id: {}", id);
        return new HashSet<>(dataDao.getDataByDataSetInfoId(id));
    }

    @Override
    public Long getRandomDataId(Integer dataSetInfoId) {
        log.debug("Querying random data id for dataSetInfoId: {}", dataSetInfoId);
        return dataDao.getRandomDataId(dataSetInfoId);
    }

    @Override
    public Data getRandomDataRow(Integer dataSetInfoId) {
        log.debug("Querying random data row for dataSetInfoId: {}", dataSetInfoId);
        return dataDao.getRandomDataRow(dataSetInfoId);
    }

    @Override
    public Data getBindingDataRow(Integer dataSetInfoId, String bindId) {
        log.debug("Querying binding data row for dataSetInfoId: {}, bindId: {}", dataSetInfoId, bindId);
        return dataDao.getBindingDataRow(dataSetInfoId, bindId);
    }

}
