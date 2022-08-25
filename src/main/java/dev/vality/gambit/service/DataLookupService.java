package dev.vality.gambit.service;

import dev.vality.gambit.domain.tables.pojos.DataLookup;

import java.util.Map;
import java.util.Set;

public interface DataLookupService {

    void saveBatch(Set<DataLookup> batch);

    Map<Integer, Long> getDataIds(Set<Integer> dataSetInfoIds, int hash);

}
