package dev.vality.gambit.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Set;

@Data
@AllArgsConstructor
public class DataEntries {

    List<String> headers;

    Set<String> values;

}
