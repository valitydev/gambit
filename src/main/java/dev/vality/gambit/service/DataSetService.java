package dev.vality.gambit.service;

import dev.vality.gambit.DataSetNotFound;

import java.io.BufferedReader;

public interface DataSetService {

    void createDataSet(String dataSetName, BufferedReader bufferedReader);

    void updateDataSet(String dataSetName, BufferedReader bufferedReader) throws DataSetNotFound;

}
