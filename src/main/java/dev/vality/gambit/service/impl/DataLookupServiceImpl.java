package dev.vality.gambit.service.impl;

import dev.vality.gambit.dao.DataLookupDao;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.service.DataLookupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class DataLookupServiceImpl implements DataLookupService {

    private final DataLookupDao dataLookupDao;

    @Override
    public void saveBatch(Set<DataLookup> batch) {
        log.debug("Save data lookup batch. Size: {}",batch.size());
        dataLookupDao.saveBatch(batch);
    }

    @Override
    public Map<Integer, Long> getDataIds(Set<Integer> dataSetInfoIds, int hash) {
        log.debug("Querying for data ids. dataSetInfoIds: {}, hash: {}", dataSetInfoIds, hash);
        return dataLookupDao.getDataLookups(dataSetInfoIds, hash).stream()
                .collect(Collectors.toMap(DataLookup::getDataSetInfoId, DataLookup::getDataId));
    }
}
