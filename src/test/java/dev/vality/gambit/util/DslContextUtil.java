package dev.vality.gambit.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DslContextUtil {

    public static <T extends Record> void truncate(DSLContext dslContext, Table<T> table) {
        dslContext.truncate(table).execute();
    }

    public static <T extends Record> int count(DSLContext dslContext, Table<T> table) {
        return dslContext.select()
                .from(table)
                .execute();
    }

}
