package dev.vality.gambit.service;

import dev.vality.gambit.domain.tables.pojos.Data;

import java.util.List;
import java.util.Set;

public interface DataService {

    void saveDataBatch(List<Data> batch);

    Set<Data> getDataByIds(Set<Long> ids);

    Set<Data> getDataByDataSetInfoId(Integer id);

    Long getRandomDataId(Integer dataSetInfoId);

    Data getRandomDataRow(Integer dataSetInfoId);

    Data getBindingDataRow(Integer dataSetInfoId, String bindId);

}
