package dev.vality.gambit.dao;

import dev.vality.gambit.domain.tables.pojos.Data;

import java.util.List;
import java.util.Set;

public interface DataDao {

    void saveBatch(List<Data> batch);

    List<Data> getDataByIds(Set<Long> ids);

    Long getRandomDataId(Integer dataSetInfoId);

    Data getRandomDataRow(Integer dataSetInfoId);

    Data getBindingDataRow(Integer dataSetInfoId, String bindId);

}
