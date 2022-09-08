package dev.vality.gambit.dao.impl;

import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.gambit.dao.DataDao;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.exception.NotFoundException;
import dev.vality.mapper.RecordRowMapper;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static dev.vality.gambit.domain.tables.Data.DATA;

@Component
public class DataDaoImpl extends AbstractGenericDao implements DataDao {

    private final RowMapper<Data> rowMapper;
    private final RowMapper<Long> idRowMapper;

    public DataDaoImpl(DataSource dataSource) {
        super(dataSource);
        this.rowMapper = new RecordRowMapper<>(DATA, Data.class);
        this.idRowMapper = new SingleColumnRowMapper<>(Long.class);
    }

    @Transactional
    @Override
    public void saveBatch(List<Data> batch) {
        List<Query> queries = batch.stream()
                .map(data -> getDslContext().newRecord(DATA, data))
                .map(dataRecord -> getDslContext().insertInto(DATA).set(dataRecord)
                        .onConflict(DATA.DATA_SET_INFO_ID, DSL.md5(DATA.VALUES))
                        .doNothing()
                )
                .collect(Collectors.toList());
        batchExecute(queries);
    }

    @Override
    public List<Data> getDataByIds(Set<Long> ids) {
        Query query = getDslContext().selectFrom(DATA)
                .where(DATA.ID.in(ids));
        return Optional.ofNullable(fetch(query, rowMapper))
                .filter(dataList -> !CollectionUtils.isEmpty(dataList))
                .orElseThrow(() -> new NotFoundException("Data entity not found, ids: " + ids));
    }

    @Override
    public Long getRandomDataId(Integer dataSetInfoId) {
        Query query = getDslContext().select(DATA.ID)
                .from(DATA)
                .where(DATA.DATA_SET_INFO_ID.eq(dataSetInfoId))
                .orderBy(DSL.rand())
                .limit(1);
        return Optional.ofNullable(fetchOne(query, idRowMapper))
                .orElseThrow(() -> new NotFoundException("Data entity not found, dataSetInfoId: " + dataSetInfoId));
    }

}
