package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.DataLookup;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataLookupFactory {

    public static DataLookup create(Integer dataSetInfoId, Long dataId, int hash) {
        var dataLookup = new DataLookup();
        dataLookup.setDatasetInfoId(dataSetInfoId);
        dataLookup.setDataId(dataId);
        dataLookup.setHash(hash);
        return dataLookup;
    }

}
