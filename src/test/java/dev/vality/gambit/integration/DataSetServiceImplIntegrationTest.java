package dev.vality.gambit.integration;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.service.impl.DataSetServiceImpl;
import dev.vality.gambit.util.JdbcUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.DigestUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
public class DataSetServiceImplIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private DataSetServiceImpl service;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private final Set<Long> allIdsRange = new HashSet<>();

    @BeforeEach
    void setUp() {
        cleanUpDb();
        assertDataBaseCounts(0, 0);
        for (int i = 1; i < 1_000; i++) {
            allIdsRange.add((long) i);
        }
    }

    @Test
    void createDataSetDataSetAlreadyExist() {
        dataSetInfoDao.save(TestObjectFactory.createDefaultDataSetInfo());
        assertThrows(
                DataSetInfoAlreadyExistException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("create.csv"))
        );
    }

    @Test
    void createDataSetWrongContentType() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("create.csv", "application/json"))
        );
    }

    @Test
    void createDataSetEmptyFile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("empty.csv"))
        );
    }

    @Test
    void createDataSetHeadersOnlyFile() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("headers_only.csv"))
        );
    }

    @Test
    void createDataSetInvalidValue() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.createDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("create_invalid_value.csv"))
        );
    }

    @Test
    void createDataSet() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );

        assertDataBaseCounts(1, 2);
        DataSetInfo actualDataSetInfo = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_NAME).get();
        assertNotNull(actualDataSetInfo.getId());
        assertEquals(TestObjectFactory.DATA_SET_INFO_NAME, actualDataSetInfo.getName());
        assertEquals(TestObjectFactory.DATA_SET_INFO_HEADERS, actualDataSetInfo.getHeaders());

        List<Data> actualData = dataDao.getDataByIds(allIdsRange);
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
                        TestObjectFactory.createMultipartFile("update.csv"))
        );
    }

    @Test
    void updateDataSetWrongContentType() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("update.csv", "application/json"))
        );
    }

    @Test
    void updateDataSetEmptyFile() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("empty.csv"))
        );
    }

    @Test
    void updateDataSetHeadersOnlyFile() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("headers_only.csv"))
        );
    }

    @Test
    void updateDataInvalidHeaders() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("update_invalid_value.csv"))
        );
    }

    @Test
    void updateDataSetInvalidValue() {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_NAME,
                TestObjectFactory.createMultipartFile("create.csv")
        );
        assertThrows(
                IllegalArgumentException.class,
                () -> service.updateDataSet(
                        TestObjectFactory.DATA_SET_INFO_NAME,
                        TestObjectFactory.createMultipartFile("update_invalid_value.csv"))
        );
    }

    @Test
    void updateDataSet() throws DataSetNotFound {
        service.createDataSet(
                TestObjectFactory.DATA_SET_INFO_IP_NAME,
                TestObjectFactory.createMultipartFile("ip_create.csv")
        );
        assertDataBaseCounts(1, 3);

        service.updateDataSet(
                TestObjectFactory.DATA_SET_INFO_IP_NAME,
                TestObjectFactory.createMultipartFile("ip_update.csv")
        );
        assertDataBaseCounts(1, 4);

        DataSetInfo actualDataSetInfo = dataSetInfoDao.getByName(TestObjectFactory.DATA_SET_INFO_IP_NAME).get();
        assertNotNull(actualDataSetInfo.getId());
        assertEquals(TestObjectFactory.DATA_SET_INFO_IP_NAME, actualDataSetInfo.getName());
        assertEquals(TestObjectFactory.DATA_SET_INFO_IP_HEADERS, actualDataSetInfo.getHeaders());

        List<Data> actualData = dataDao.getDataByIds(allIdsRange);
        assertEquals(4, actualData.size());
        Map<String, Data> expected = Map.of(
                "127.0.0.1", createExpectedData(actualDataSetInfo.getId(), "127.0.0.1"),
                "234.123.43.2", createExpectedData(actualDataSetInfo.getId(), "234.123.43.2"),
                "65.33.75.235", createExpectedData(actualDataSetInfo.getId(), "65.33.75.235"),
                "17.22.4.1", createExpectedData(actualDataSetInfo.getId(), "17.22.4.1")
        );
        actualData.forEach(actual -> assertData(expected.get(actual.getValues()), actual));
    }

    private void assertDataBaseCounts(int dataSetInfoCount, int dataCount) {
        assertEquals(0, JdbcUtil.count(jdbcTemplate, "data_lookup"));
        assertEquals(dataSetInfoCount, JdbcUtil.count(jdbcTemplate, "data_set_info"));
        assertEquals(dataCount, JdbcUtil.count(jdbcTemplate, "data"));
    }

    private Data createExpectedData(Integer dataSetInfoId, String value) {
        return new Data(null, dataSetInfoId, value, DigestUtils.md5DigestAsHex(value.getBytes()));
    }

    private void assertData(Data expected, Data actual) {
        assertNotNull(actual.getId());
        assertEquals(expected.getDataSetInfoId(), actual.getDataSetInfoId());
        assertEquals(expected.getValues(), actual.getValues());
        assertEquals(expected.getValuesHash(), actual.getValuesHash());
    }

}
