package dev.vality.gambit.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Objects;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JdbcUtil {

    // TODO: переписать используя DslContext

    public static void truncate(JdbcTemplate jdbcTemplate, String table) {
        jdbcTemplate.execute("truncate table gbt." + table);
    }

    public static int count(NamedParameterJdbcTemplate jdbcTemplate, String table) {
        String query = "select count(1) from gbt." + table;
        return Objects.requireNonNull(
                jdbcTemplate.queryForObject(query, new MapSqlParameterSource(), Integer.class));
    }

}
