package dev.vality.gambit.integration;

import dev.vality.gambit.DataRequest;
import dev.vality.gambit.DataResponse;
import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import dev.vality.gambit.service.impl.StubDataServiceHandler;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static dev.vality.gambit.util.TestObjectFactory.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class StubDataServiceHandlerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private StubDataServiceHandler handler;

    @BeforeEach
    void setUp() {
        cleanUpDb();
    }

    @Test
    void getDataOneDataSetInfoNotExisting() {
        fillDb(
                List.of(createDefaultDataSetInfo()),
                List.of(createData(DATA_SET_INFO_ID, "uno,dos,tres", "hash!")),
                null
        );
        int hash = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        assertTrue(CollectionUtils.isEmpty(dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID), hash)));
        assertThrows(
                DataSetNotFound.class,
                () -> handler.getData(new DataRequest()
                        .setDataSetsNames(Set.of(DATA_SET_INFO_NAME, "non_existing"))
                        .setLookupKey(hash))
        );
    }

    @Test
    void getDataTwoDataSetsBothHaveLookup() throws TException {
        int hash = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        DataLookup firstDataLookup = new DataLookup(1L, DATA_SET_INFO_ID, 1L, hash);
        DataLookup secondDataLookup = new DataLookup(2L, DATA_SET_INFO_IP_ID, 2L, hash);
        fillDb(
                List.of(createDefaultDataSetInfo(),
                        new DataSetInfo(DATA_SET_INFO_IP_ID, DATA_SET_INFO_IP_NAME, DATA_SET_INFO_IP_HEADERS)),
                List.of(new Data(1L, DATA_SET_INFO_ID, "uno,dos,tres", "hash!"),
                        new Data(2L, DATA_SET_INFO_IP_ID, "127.212.54.3", "хеш!")),
                Set.of(firstDataLookup, secondDataLookup)
        );
        List<DataLookup> preparedDataLookups =
                dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID, DATA_SET_INFO_IP_ID), hash);
        assertEquals(2, preparedDataLookups.size());
        assertTrue(preparedDataLookups.contains(firstDataLookup));
        assertTrue(preparedDataLookups.contains(secondDataLookup));

        DataResponse actual = handler.getData(new DataRequest()
                .setDataSetsNames(Set.of(DATA_SET_INFO_NAME, DATA_SET_INFO_IP_NAME))
                .setLookupKey(hash)
        );
        List<DataLookup> actualDataLookups =
                dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID, DATA_SET_INFO_IP_ID), hash);
        assertEquals(preparedDataLookups, actualDataLookups);
        Map<String, String> expected = Map.of(
                "h1", "uno",
                "h2", "dos",
                "h3", "tres",
                "ip", "127.212.54.3"
        );
        assertEquals(expected, actual.getData());
    }

    @Test
    void getDataTwoDataSetsOneHasLookup() throws TException {
        int hash = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        DataLookup firstDataLookup = new DataLookup(835467L, DATA_SET_INFO_ID, 1L, hash);
        fillDb(
                List.of(createDefaultDataSetInfo(),
                        new DataSetInfo(DATA_SET_INFO_IP_ID, DATA_SET_INFO_IP_NAME, DATA_SET_INFO_IP_HEADERS)),
                List.of(new Data(1L, DATA_SET_INFO_ID, "uno,dos,tres", "hash!"),
                        new Data(2L, DATA_SET_INFO_IP_ID, "127.212.54.3", "хеш!")),
                Set.of(firstDataLookup)
        );
        List<DataLookup> preparedDataLookups =
                dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID, DATA_SET_INFO_IP_ID), hash);
        assertEquals(1, preparedDataLookups.size());
        assertTrue(preparedDataLookups.contains(firstDataLookup));

        DataResponse actual = handler.getData(new DataRequest()
                .setDataSetsNames(Set.of(DATA_SET_INFO_NAME, DATA_SET_INFO_IP_NAME))
                .setLookupKey(hash)
        );
        Map<String, String> expected = Map.of(
                "h1", "uno",
                "h2", "dos",
                "h3", "tres",
                "ip", "127.212.54.3"
        );
        assertEquals(expected, actual.getData());
        List<DataLookup> actualDataLookups =
                dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID, DATA_SET_INFO_IP_ID), hash);
        assertTrue(actualDataLookups.contains(firstDataLookup));
        DataLookup newDataLookup = actualDataLookups.stream()
                .filter(dataLookup -> dataLookup.getDataSetInfoId().equals(DATA_SET_INFO_IP_ID))
                .findFirst()
                .orElseThrow();
        assertNotNull(newDataLookup.getId());
        assertEquals(DATA_SET_INFO_IP_ID, newDataLookup.getDataSetInfoId());
        assertEquals(2L, newDataLookup.getDataId());
        assertEquals(hash, newDataLookup.getKey());
    }

    @Test
    void getDataOneDataSetOneDataEntryNoLookup() throws TException {
        fillDb(
                List.of(createDefaultDataSetInfo()),
                List.of(createData(DATA_SET_INFO_ID, "uno,dos,tres", "hash!")),
                null
        );
        int hash = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
        assertTrue(CollectionUtils.isEmpty(dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID), hash)));
        DataResponse actual = handler.getData(new DataRequest()
                .setDataSetsNames(Set.of(DATA_SET_INFO_NAME))
                .setLookupKey(hash)
        );
        Map<String, String> expected = Map.of(
                "h1", "uno",
                "h2", "dos",
                "h3", "tres"
        );
        assertEquals(expected, actual.getData());
        assertNotNull(dataLookupDao.getDataLookups(Set.of(DATA_SET_INFO_ID), hash));
    }

}