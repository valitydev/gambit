package dev.vality.gambit.dao.impl;

import dev.vality.dao.impl.AbstractGenericDao;
import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.dao.DataSetInfoDao;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.mapper.RecordRowMapper;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Query;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static dev.vality.gambit.domain.tables.DataSetInfo.DATA_SET_INFO;

@Slf4j
@Component
public class DataSetInfoDaoImpl extends AbstractGenericDao implements DataSetInfoDao {

    private final RowMapper<DataSetInfo> rowMapper;

    public DataSetInfoDaoImpl(DataSource dataSource) {
        super(dataSource);
        this.rowMapper = new RecordRowMapper<>(DATA_SET_INFO, DataSetInfo.class);
    }

    @Transactional
    @Override
    public Integer save(DataSetInfo dataSetInfo) {
        Query query = getDslContext().insertInto(DATA_SET_INFO)
                .set(getDslContext().newRecord(DATA_SET_INFO, dataSetInfo))
                .returning(DATA_SET_INFO.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        execute(query, keyHolder);
        return Objects.requireNonNull(keyHolder.getKey()).intValue();
    }

    @Override
    public DataSetInfo getByName(String name) throws DataSetNotFound {
        Query query = getDslContext().selectFrom(DATA_SET_INFO)
                .where(DATA_SET_INFO.NAME.eq(name));
        return Optional.ofNullable(fetchOne(query, rowMapper))
                .orElseThrow(DataSetNotFound::new);
    }

    @Override
    public List<DataSetInfo> getByNames(Set<String> names) throws DataSetNotFound {
        Query query = getDslContext().selectFrom(DATA_SET_INFO)
                .where(DATA_SET_INFO.NAME.in(names));
        List<DataSetInfo> dataSetInfos = fetch(query, rowMapper);
        if (CollectionUtils.isEmpty(dataSetInfos) || dataSetInfos.size() != names.size()) {
            log.error("Some or all of DataSetInfo entities are not found. names: {}, dataSetInfos: {}",
                    names, dataSetInfos);
            throw new DataSetNotFound();
        }
        return dataSetInfos;
    }
}
