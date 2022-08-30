package dev.vality.gambit.dao;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface DataSetInfoDao {

    Integer save(DataSetInfo dataSetInfo);

    Optional<DataSetInfo> getByName(String name);

    List<DataSetInfo> getByNames(Set<String> names) throws DataSetNotFound;

}
