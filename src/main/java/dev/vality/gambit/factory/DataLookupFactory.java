package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.DataLookup;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataLookupFactory {

    public static DataLookup create(Integer dataSetInfoId, Long dataId, int key) {
        var dataLookup = new DataLookup();
        dataLookup.setDataSetInfoId(dataSetInfoId);
        dataLookup.setDataId(dataId);
        dataLookup.setKey(key);
        return dataLookup;
    }

}
