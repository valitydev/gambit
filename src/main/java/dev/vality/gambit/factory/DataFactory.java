package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.Data;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.util.DigestUtils;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataFactory {

    public static Data create(Integer dataSetInfoId, String values) {
        var data = new Data();
        data.setDataSetInfoId(dataSetInfoId);
        data.setValues(values);
        data.setValuesHash(DigestUtils.md5DigestAsHex(values.getBytes()));
        return data;
    }
}
