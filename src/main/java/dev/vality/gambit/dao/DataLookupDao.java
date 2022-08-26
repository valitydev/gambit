package dev.vality.gambit.dao;

import dev.vality.gambit.domain.tables.pojos.DataLookup;

import java.util.List;
import java.util.Set;

public interface DataLookupDao {

    void saveBatch(Set<DataLookup> batch);

    List<DataLookup> getDataLookups(Set<Integer> dataSetInfoIds, int hash);

}
