package dev.vality.gambit.factory;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataMapFactoryTest {

    private final String headers = "one,two,three";
    private final String values = "uno,dos,tres";

    @Test
    void createDataMapEmptyInput() {
        assertThrows(IllegalArgumentException.class, () -> DataMapFactory.createDataMap(null, null));
        assertThrows(IllegalArgumentException.class, () -> DataMapFactory.createDataMap(headers, ""));
        assertThrows(IllegalArgumentException.class, () -> DataMapFactory.createDataMap("", values));
    }

    @Test
    void createDataMapSplitError() {
        assertThrows(IllegalArgumentException.class, () -> DataMapFactory.createDataMap("1", "1,2"));
        assertThrows(IllegalArgumentException.class, () -> DataMapFactory.createDataMap("one,two", "tres"));
    }

    @Test
    void createDataMap() {
        Map<String, String> expected = Map.of(
                "one", "uno",
                "two", "dos",
                "three", "tres"
        );
        Map<String, String> actual = DataMapFactory.createDataMap(headers, values);
        assertEquals(expected, actual);
    }
}