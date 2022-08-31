package dev.vality.gambit.service;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface DataSetInfoService {

    Integer createDataSetInfo(DataSetInfo dataSetInfo);

    Map<Integer, DataSetInfo> getDataSetInfoByNames(Set<String> dataSetInfoNames) throws DataSetNotFound;

    Optional<DataSetInfo> getDataSetInfoByName(String dataSetInfoName);

}
