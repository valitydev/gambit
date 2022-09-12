package dev.vality.gambit.dao.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.Tables;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.util.DslContextUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class DataSetInfoDaoImplTest {

    @Autowired
    private DataSetInfoDaoImpl dataSetInfoDao;

    @Autowired
    private DSLContext dslContext;

    @BeforeEach
    void setUp() {
        DslContextUtil.truncate(dslContext, Tables.DATA_SET_INFO);
    }

    @Test
    void save() {
        DataSetInfo dataSetInfo = TestObjectFactory.createDefaultDataSetInfo();
        dataSetInfoDao.save(dataSetInfo);
        Optional<DataSetInfo> actual = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME);
        assertEquals(dataSetInfo, actual.get());
    }

    @Test
    void getByNameNotPresent() {
        DataSetInfo dataSetInfo = TestObjectFactory.createDefaultDataSetInfo();
        dataSetInfoDao.save(dataSetInfo);
        assertEquals(Optional.empty(), dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_IP_NAME));
    }

    @Test
    void getByName() {
        DataSetInfo dataSetInfo = TestObjectFactory.createDefaultDataSetInfo();
        dataSetInfoDao.save(dataSetInfo);
        assertEquals(dataSetInfo, dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME).get());
    }

    @Test
    void getByNamesDataSetNotFound() {
        List<DataSetInfo> dataSetInfoList = TestObjectFactory.createDataSetInfoList(10);
        dataSetInfoList.forEach(dataSetInfo -> dataSetInfoDao.save(dataSetInfo));
        assertThrows(DataSetNotFound.class, () -> dataSetInfoDao.getByNames(Set.of("te", "st")));
    }

    @Test
    void getByNamesNotAllDataSetsFound() {
        List<DataSetInfo> dataSetInfoList = TestObjectFactory.createDataSetInfoList(10);
        dataSetInfoList.forEach(dataSetInfo -> dataSetInfoDao.save(dataSetInfo));
        assertThrows(DataSetNotFound.class, () -> dataSetInfoDao.getByNames(Set.of("1", "st")));
    }

    @Test
    void getByNames() throws DataSetNotFound {
        List<DataSetInfo> dataSetInfoList = TestObjectFactory.createDataSetInfoList(10);
        dataSetInfoList.forEach(dataSetInfo -> dataSetInfoDao.save(dataSetInfo));
        dataSetInfoDao.getByNames(Set.of("1", "2"))
                .forEach(actualDataSetInfo -> assertTrue(dataSetInfoList.contains(actualDataSetInfo)));
    }
}