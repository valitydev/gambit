package dev.vality.gambit.dao.impl;

import dev.vality.gambit.annotation.SpringBootPostgresqlTest;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.util.JdbcUtil;
import dev.vality.gambit.util.TestObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootPostgresqlTest
class DataLookupDaoImplTest {

    @Autowired
    private DataLookupDaoImpl dataLookupDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @BeforeEach
    void setUp() {
        JdbcUtil.truncate(jdbcTemplate, "data_lookup");
    }

    @Test
    void saveBatch() {
        List<DataLookup> dataLookupList = TestObjectFactory.createDataLookupList(2425243, 50);
        dataLookupDao.saveBatch(new HashSet<>(dataLookupList));
        assertEquals(50, JdbcUtil.count(namedParameterJdbcTemplate, "data_lookup"));
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