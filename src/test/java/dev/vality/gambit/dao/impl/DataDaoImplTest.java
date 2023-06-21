package dev.vality.gambit.dao.impl;

import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.Tables;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.exception.NotFoundException;
import dev.vality.gambit.util.DslContextUtil;
import dev.vality.gambit.util.TestObjectFactory;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootPostgresqlTest
class DataDaoImplTest {

    @Autowired
    private DataDaoImpl dataDao;

    @Autowired
    private DSLContext dslContext;

    @BeforeEach
    void setUp() {
        DslContextUtil.truncate(dslContext, Tables.DATA);
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
        dataDao.saveBatch(List.of(new Data(55L, TestObjectFactory.DATA_SET_INFO_ID, "55")));
        Long randomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        assertEquals(55L, randomDataId);
    }

    @Test
    void getRandomDataId() {
        dataDao.saveBatch(TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 50));
        Long firstRandomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        Long secondRandomDataId = dataDao.getRandomDataId(TestObjectFactory.DATA_SET_INFO_ID);
        assertNotEquals(firstRandomDataId, secondRandomDataId);
        assertTrue(0 < firstRandomDataId && firstRandomDataId < 51);
        assertTrue(0 < secondRandomDataId && secondRandomDataId < 51);
    }

    @Disabled
    @Test
    void getRandomDataIdPerformanceTest() {
        log.info("Preparing DB");
        int entriesPerDataSet = 1_000_000;
        int dataSetsCount = 3;
        for (int i = 1; i <= dataSetsCount; i++) {
            log.info("Preparing data set #{}", i);
            List<Data> data = new ArrayList<>();
            for (int j = 0; j < entriesPerDataSet; j++) {
                data.add(TestObjectFactory.createData(i, String.valueOf(i)));
            }
            log.info("Inserting data set #{}", i);
            dataDao.saveBatch(data);
            log.info("Data set #{} inserted", i);
        }

        log.info("Start querying DB for random ID");
        Set<Long> randomIds = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            long start = System.currentTimeMillis();
            Long randomId = dataDao.getRandomDataId(ThreadLocalRandom.current().nextInt(1, dataSetsCount + 1));
            long end = System.currentTimeMillis();
            randomIds.add(randomId);
            log.info("{}th query randomId: {}, execution time: {}ms", i, randomId, end - start);
        }
        assertEquals(500, randomIds.size());
        randomIds.forEach(randomId -> assertTrue(0 <= randomId && randomId <= dataSetsCount * entriesPerDataSet));
    }

    @Test
    void getRandomDataRow() {
        dataDao.saveBatch(TestObjectFactory.createDataList(TestObjectFactory.DATA_SET_INFO_ID, 50));
        Data firstRandomData = dataDao.getRandomDataRow(TestObjectFactory.DATA_SET_INFO_ID);
        Data secondRandomData = dataDao.getRandomDataRow(TestObjectFactory.DATA_SET_INFO_ID);
        assertNotEquals(firstRandomData, secondRandomData);
        assertTrue(0 < firstRandomData.getId() && firstRandomData.getId() < 51);
        assertTrue(0 < secondRandomData.getId() && secondRandomData.getId() < 51);
    }

    @Disabled
    @Test
    void getRandomDataRowPerformanceTest() {
        log.info("Preparing DB");
        int entriesPerDataSet = 1_000_000;
        int dataSetsCount = 3;
        for (int i = 1; i <= dataSetsCount; i++) {
            log.info("Preparing data set #{}", i);
            List<Data> data = new ArrayList<>();
            for (int j = 0; j < entriesPerDataSet; j++) {
                data.add(TestObjectFactory.createData(i, TestObjectFactory.randomString()));
            }
            log.info("Inserting data set #{}", i);
            dataDao.saveBatch(data);
            log.info("Data set #{} inserted", i);
        }

        log.info("Start querying DB for random ID");
        List<Long> randomIds = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            long start = System.currentTimeMillis();
            int dataSetInfoId = ThreadLocalRandom.current().nextInt(1, dataSetsCount + 1);
            var randomDataRow = dataDao.getRandomDataRow(dataSetInfoId);
            long end = System.currentTimeMillis();
            randomIds.add(randomDataRow.getId());
            log.info("{}th query randomDataRow: {}, execution time: {}ms", i, randomDataRow, end - start);
        }
        assertEquals(500, randomIds.size());
        randomIds.forEach(randomId -> assertTrue(0 <= randomId && randomId <= dataSetsCount * entriesPerDataSet));
    }
}