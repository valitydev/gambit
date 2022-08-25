package dev.vality.gambit.service;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;

import java.util.List;
import java.util.Map;

public interface DataSetInfoService {

    Integer createDataSetInfo(DataSetInfo dataSetInfo);

    Map<Integer, DataSetInfo> getDataSetInfoByNames(List<String> dataSetInfoNames) throws DataSetNotFound;

}
