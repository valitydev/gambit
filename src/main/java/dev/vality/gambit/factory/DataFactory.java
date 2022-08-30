package dev.vality.gambit.factory;

import dev.vality.gambit.domain.tables.pojos.Data;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.util.DigestUtils;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DataFactory {

    public static Data create(Integer dataSetInfoId, String value) {
        return new Data(null, dataSetInfoId, value, DigestUtils.md5DigestAsHex(value.getBytes()));
    }
}
