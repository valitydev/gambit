package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.Data;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataFactory {

    public static Data create(Integer dataSetInfoId, String values) {
        var data = new Data();
        data.setDataSetInfoId(dataSetInfoId);
        data.setValues(values);
        return data;
    }
}
