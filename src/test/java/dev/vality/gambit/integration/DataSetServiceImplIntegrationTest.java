package dev.vality.gambit.integration;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.Tables;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.service.impl.DataSetServiceImpl;
import dev.vality.gambit.util.TestObjectFactory;
import dev.vality.mapper.RecordRowMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
public class DataSetServiceImplIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private DataSetServiceImpl service;

    private final RowMapper<Data> rowMapper = new RecordRowMapper<>(Tables.DATA, Data.class);

    @BeforeEach
    void setUp() {
        cleanUpDb();
        assertDataBaseCounts(0, 0);
    }

    @Test
    void createDataSetDataSetAlreadyExist() {
        dataSetInfoDao.save(TestObjectFactory.createDefaultDataSetInfo());
        assertThrows(
                DataSetInfoAlreadyExistException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("create.csv"))
        );
    }

    @Test
    void createDataSetEmptyFile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("empty.csv"))
        );
    }

    @Test
    void createDataSetHeadersOnlyFile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("headers_only.csv"))
        );
    }

    @Test
    void createDataSetInvalidValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("create_invalid_value.csv"))
        );
    }

    @Test
    void createDataSet() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createBufferedReader("create.csv")
        );

        assertDataBaseCounts(1, 2);
        DataSetInfo actualDataSetInfo = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME).get();
        assertNotNull(actualDataSetInfo.getId());
        assertEquals(TestObjectFactory.DATA_SET_INFO_NAME, actualDataSetInfo.getName());
        assertEquals(TestObjectFactory.DATA_SET_INFO_HEADERS, actualDataSetInfo.getHeaders());

        List<Data> actualData = dataDao.fetch(dslContext.selectFrom(Tables.DATA), rowMapper);
        assertEquals(2, actualData.size());
        Map<String, Data> expected = Map.of(
                "uno,dos,tres", createExpectedData(actualDataSetInfo.getId(), "uno,dos,tres"),
                "бер,ике,оч", createExpectedData(actualDataSetInfo.getId(), "бер,ике,оч")
        );
        actualData.forEach(actual -> assertData(expected.get(actual.getValues()), actual));
    }

    @Test
    void updateDataSetDataSetNotFound() {
        assertThrows(
                DataSetNotFound.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("update.csv"))
        );
    }

    @Test
    void updateDataSetEmptyFile() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createBufferedReader("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("empty.csv"))
        );
    }

    @Test
    void updateDataSetHeadersOnlyFile() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createBufferedReader("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("headers_only.csv"))
        );
    }

    @Test
    void updateDataInvalidHeaders() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createBufferedReader("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("update_invalid_value.csv"))
        );
    }

    @Test
    void updateDataSetInvalidValue() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createBufferedReader("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createBufferedReader("update_invalid_value.csv"))
        );
    }

    @Test
    void updateDataSet() throws DataSetNotFound {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_IP_NAME,
                TestObjectFactory.createBufferedReader("ip_create.csv")
        );
        assertDataBaseCounts(1, 3);

        service.updateDataSet(
                TestObjectFactory.DATA_SET_INFO_IP_NAME,
                TestObjectFactory.createBufferedReader("ip_update.csv")
        );
        assertDataBaseCounts(1, 4);

        DataSetInfo actualDataSetInfo = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_IP_NAME).get();
        assertNotNull(actualDataSetInfo.getId());
        assertEquals(TestObjectFactory.DATA_SET_INFO_IP_NAME, actualDataSetInfo.getName());
        assertEquals(TestObjectFactory.DATA_SET_INFO_IP_HEADERS, actualDataSetInfo.getHeaders());

        List<Data> actualData = dataDao.fetch(dslContext.selectFrom(Tables.DATA), rowMapper);
        assertEquals(4, actualData.size());
        Map<String, Data> expected = Map.of(
                "127.0.0.1", createExpectedData(actualDataSetInfo.getId(), "127.0.0.1"),
                "234.123.43.2", createExpectedData(actualDataSetInfo.getId(), "234.123.43.2"),
                "65.33.75.235", createExpectedData(actualDataSetInfo.getId(), "65.33.75.235"),
                "17.22.4.1", createExpectedData(actualDataSetInfo.getId(), "17.22.4.1")
        );
        actualData.forEach(actual -> assertData(expected.get(actual.getValues()), actual));
    }

    private Data createExpectedData(Integer dataSetInfoId, String value) {
        return new Data(null, dataSetInfoId, value);
    }

    private void assertData(Data expected, Data actual) {
        assertNotNull(actual.getId());
        assertEquals(expected.getDataSetInfoId(), actual.getDataSetInfoId());
        assertEquals(expected.getValues(), actual.getValues());
    }

}
