package dev.vality.gambit.dao.impl;

import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.gambit.dao.DataLookupDao;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.mapper.RecordRowMapper;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static dev.vality.gambit.domain.tables.DataLookup.DATA_LOOKUP;

@Component
public class DataLookupDaoImpl extends AbstractGenericDao implements DataLookupDao {

    private final RowMapper<DataLookup> rowMapper;

    public DataLookupDaoImpl(DataSource dataSource) {
        super(dataSource);
        this.rowMapper = new RecordRowMapper<>(DATA_LOOKUP, DataLookup.class);
    }

    @Transactional
    @Override
    public void saveBatch(Set<DataLookup> batch) {
        List<Query> queries = batch.stream()
                .map(dataLookup -> getDslContext().insertInto(DATA_LOOKUP)
                        .set(getDslContext().newRecord(DATA_LOOKUP, dataLookup)))
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    @Override
    public List<DataLookup> getDataLookups(Set<Integer> dataSetInfoIds, int key) {
        Query query = getDslContext().selectFrom(DATA_LOOKUP)
                .where(DATA_LOOKUP.DATA_SET_INFO_ID.in(dataSetInfoIds)
                        .and(DATA_LOOKUP.KEY.eq(key)));
        return fetch(query, rowMapper);
    }

}
