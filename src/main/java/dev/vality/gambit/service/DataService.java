package dev.vality.gambit.service;

import dev.vality.gambit.domain.tables.pojos.Data;

import java.util.List;
import java.util.Set;

public interface DataService {

    void saveDataBatch(List<Data> batch);

    Set<Data> getDataByIds(Set<Long> ids);

    Long getRandomDataId(Integer dataSetInfoId);

}
