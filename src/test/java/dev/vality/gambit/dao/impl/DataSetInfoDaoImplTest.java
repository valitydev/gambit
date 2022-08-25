package dev.vality.gambit.dao.impl;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.util.JdbcUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class DataSetInfoDaoImplTest {

    @Autowired
    private DataSetInfoDaoImpl dataSetInfoDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        JdbcUtil.truncate(jdbcTemplate, "data_set_info");
    }

    @Test
    void save() throws DataSetNotFound {
        DataSetInfo dataSetInfo = TestObjectFactory.createDefaultDataSetInfo();
        dataSetInfoDao.save(dataSetInfo);
        DataSetInfo actual = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME);
        assertEquals(dataSetInfo, actual);
    }

    @Test
    void getByNameDataSetNotFound() {
        dataSetInfoDao.save(TestObjectFactory.createDefaultDataSetInfo());
        assertThrows(DataSetNotFound.class, () -> dataSetInfoDao.getByName("test"));
    }

    @Test
    void getByName() throws DataSetNotFound {
        DataSetInfo dataSetInfo = TestObjectFactory.createDefaultDataSetInfo();
        dataSetInfoDao.save(dataSetInfo);
        assertEquals(dataSetInfo, dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME));
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