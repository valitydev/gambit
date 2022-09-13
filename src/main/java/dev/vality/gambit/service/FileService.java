package dev.vality.gambit.service;

import dev.vality.gambit.model.DataEntries;

import java.io.BufferedReader;
import java.util.List;

public interface FileService {

    DataEntries process(BufferedReader bufferedReader);

    DataEntries process(BufferedReader bufferedReader, List<String> existingHeaders);

}
