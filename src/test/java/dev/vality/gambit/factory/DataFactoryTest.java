package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.Data;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataFactoryTest {

    @Test
    void create() {
        String value = "value_1233,afdf,213,#432";
        int dataSetInfoId = 2;
        Data actual = DataFactory.create(dataSetInfoId, value);
        assertNull(actual.getId());
        assertEquals(dataSetInfoId, actual.getDataSetInfoId());
        assertEquals(value, actual.getValues());
    }
}