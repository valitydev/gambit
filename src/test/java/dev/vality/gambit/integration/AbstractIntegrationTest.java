package dev.vality.gambit.integration;

import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.dao.DataDao;
import dev.vality.gambit.dao.DataLookupDao;
import dev.vality.gambit.dao.DataSetInfoDao;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.util.JdbcUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

@SpringBootPostgresqlTest
public class AbstractIntegrationTest {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    protected DataDao dataDao;

    @Autowired
    protected DataSetInfoDao dataSetInfoDao;

    @Autowired
    protected DataLookupDao dataLookupDao;

    void cleanUpDb() {
        JdbcUtil.truncate(jdbcTemplate, "data");
        JdbcUtil.truncate(jdbcTemplate, "data_lookup");
        JdbcUtil.truncate(jdbcTemplate, "data_set_info");
    }

    protected void fillDb(List<DataSetInfo> dataSetInfos, List<Data> dataList, Set<DataLookup> dataLookups) {
        if (!CollectionUtils.isEmpty(dataSetInfos)) {
            dataSetInfos.forEach(dataSetInfo -> dataSetInfoDao.save(dataSetInfo));
        }
        if (!CollectionUtils.isEmpty(dataList)) {
            dataDao.saveBatch(dataList);
        }
        if (!CollectionUtils.isEmpty(dataLookups)) {
            dataLookupDao.saveBatch(dataLookups);
        }
    }

}
