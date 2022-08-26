package dev.vality.gambit.dao;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;

import java.util.List;
import java.util.Set;

public interface DataSetInfoDao {

    Integer save(DataSetInfo dataSetInfo);

    DataSetInfo getByName(String name) throws DataSetNotFound;

    List<DataSetInfo> getByNames(Set<String> names) throws DataSetNotFound;

}
