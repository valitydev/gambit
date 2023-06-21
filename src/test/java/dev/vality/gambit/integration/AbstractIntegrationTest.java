package dev.vality.gambit.integration;

import dev.vality.gambit.dao.DataLookupDao;
import dev.vality.gambit.dao.DataSetInfoDao;
import dev.vality.gambit.dao.impl.DataDaoImpl;
import dev.vality.gambit.domain.Tables;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.util.DslContextUtil;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractIntegrationTest {

    @Autowired
    protected DSLContext dslContext;

    @Autowired
    protected DataDaoImpl dataDao;

    @Autowired
    protected DataSetInfoDao dataSetInfoDao;

    @Autowired
    protected DataLookupDao dataLookupDao;

    void cleanUpDb() {
        DslContextUtil.truncate(dslContext, Tables.DATA);
        DslContextUtil.truncate(dslContext, Tables.DATA_LOOKUP);
        DslContextUtil.truncate(dslContext, Tables.DATA_SET_INFO);
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

    protected void assertDataBaseCounts(int dataSetInfoCount, int dataCount) {
        assertEquals(0, DslContextUtil.count(dslContext, Tables.DATA_LOOKUP));
        assertEquals(dataSetInfoCount, DslContextUtil.count(dslContext, Tables.DATA_SET_INFO));
        assertEquals(dataCount, DslContextUtil.count(dslContext, Tables.DATA));
    }

}
