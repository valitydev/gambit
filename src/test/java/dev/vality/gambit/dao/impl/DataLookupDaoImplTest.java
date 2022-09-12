package dev.vality.gambit.dao.impl;

import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.Tables;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.util.DslContextUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class DataLookupDaoImplTest {

    @Autowired
    private DataLookupDaoImpl dataLookupDao;

    @Autowired
    private DSLContext dslContext;

    @BeforeEach
    void setUp() {
        DslContextUtil.truncate(dslContext, Tables.DATA_LOOKUP);
    }

    @Test
    void saveBatch() {
        List<DataLookup> dataLookupList = TestObjectFactory.createDataLookupList(2425243, 50);
        dataLookupDao.saveBatch(new HashSet<>(dataLookupList));
        assertEquals(50, DslContextUtil.count(dslContext, Tables.DATA_LOOKUP));
    }

    @Test
    void getDataLookupsNoResults() {
        Set<DataLookup> dataLookups = Set.of(
                new DataLookup(1L, 1, 1L, 1)
        );
        dataLookupDao.saveBatch(dataLookups);
        List<DataLookup> actual = dataLookupDao.getDataLookups(Set.of(546), 1);
        assertNotNull(actual);
        assertTrue(actual.isEmpty());
    }

    @Test
    void getDataLookups() {
        Set<DataLookup> dataLookups = Set.of(
                new DataLookup(1L, 1, 1L, 1),
                new DataLookup(2L, 2, 2L, 1),
                new DataLookup(3L, 3, 3L, 1)
        );
        dataLookupDao.saveBatch(dataLookups);
        dataLookupDao.getDataLookups(Set.of(1, 3), 1)
                .forEach(dataLookup -> assertTrue(dataLookups.contains(dataLookup)));
    }
}