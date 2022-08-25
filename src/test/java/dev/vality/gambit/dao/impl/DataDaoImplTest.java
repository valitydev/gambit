package dev.vality.gambit.dao.impl;

import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.exception.NotFoundException;
import dev.vality.gambit.util.JdbcUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class DataDaoImplTest {

    @Autowired
    private DataDaoImpl dataDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        JdbcUtil.truncate(jdbcTemplate, "data");
    }

    @Test
    void saveBatch() {
        List<Data> dataList = TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 5);
        dataDao.saveBatch(dataList);
        Set<Long> dataIds = dataList.stream()
                .map(Data::getId)
                .collect(Collectors.toSet());
        assertEquals(dataList, dataDao.getDataByIds(dataIds));
    }

    @Test
    void getDataByIdsThrowsNotFoundException() {
        List<Data> dataList = TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 5);
        dataDao.saveBatch(dataList);
        assertThrows(NotFoundException.class, () -> dataDao.getDataByIds(Set.of(456L)));
    }

    @Test
    void getDataByIds() {
        List<Data> dataList = TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 5);
        dataDao.saveBatch(dataList);
        List<Data> dataByIds = dataDao.getDataByIds(Set.of(1L, 2L, 3L));
        assertEquals(dataList.subList(0, 3), dataByIds);
    }

    @Test
    void getRandomDataIdThrowsNotFoundException() {
        dataDao.saveBatch(TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 1));
        assertThrows(NotFoundException.class, () -> dataDao.getRandomDataId(3546));
    }

    @Test
    void getRandomDataIdSingleEntity() {
        dataDao.saveBatch(List.of(
                new Data(55L, TestObjectFactory.DATA_SET_INFO_ID, "55", "55")
        ));
        Long randomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        assertEquals(55L, randomDataId);
    }

    @Test
    void getRandomDataId() {
        dataDao.saveBatch(TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 50));
        Long firstRandomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        Long secondRandomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        assertNotEquals(firstRandomDataId, secondRandomDataId);
        assertTrue(0 < firstRandomDataId && firstRandomDataId < 1001);
        assertTrue(0 < secondRandomDataId && secondRandomDataId < 1001);
    }

}