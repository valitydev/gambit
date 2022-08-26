package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.DataLookup;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataLookupFactoryTest {

    @Test
    void create() {
        int dataSetInfoId = 12;
        long dataId = 1245L;
        int hash = 435116;
        DataLookup actual = DataLookupFactory.create(dataSetInfoId, dataId, hash);
        assertNull(actual.getId());
        assertEquals(dataSetInfoId, actual.getDataSetInfoId());
        assertEquals(dataId, actual.getDataId());
        assertEquals(hash, actual.getKey());
    }
}