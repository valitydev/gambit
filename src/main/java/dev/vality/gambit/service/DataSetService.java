package dev.vality.gambit.service;

import dev.vality.gambit.DataSetNotFound;
import org.springframework.web.multipart.MultipartFile;

public interface DataSetService {

    void createDataSet(String dataSetName, MultipartFile file);

    void updateDataSet(String dataSetName, MultipartFile file) throws DataSetNotFound;

}
