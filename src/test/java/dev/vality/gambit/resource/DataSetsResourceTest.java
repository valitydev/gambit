package dev.vality.gambit.resource;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.service.DataSetService;
import dev.vality.gambit.util.TestObjectFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.multipart.MultipartFile;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;

@ExtendWith({SpringExtension.class, MockitoExtension.class})
class DataSetsResourceTest {

    private DataSetsResource resource;

    @Mock
    private DataSetService dataSetService;

    private MultipartFile file;

    @BeforeEach
    void setUp() {
        this.resource = new DataSetsResource(dataSetService);
        file = TestObjectFactory.createMultipartFile("create.csv");
    }

    @Test
    void createDataSetIllegalArgumentException() {
        doThrow(new IllegalArgumentException())
                .when(dataSetService)
                .createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void createDataSetDataSetInfoAlreadyExistException() {
        doThrow(new DataSetInfoAlreadyExistException())
                .when(dataSetService)
                .createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void createDataSet() {
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
    }

    @Test
    void updateDataSetIllegalArgumentException() throws DataSetNotFound {
        doThrow(new IllegalArgumentException())
                .when(dataSetService)
                .updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateDataSetDataSetInfoAlreadyExistException() throws DataSetNotFound {
        doThrow(new DataSetNotFound())
                .when(dataSetService)
                .updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateDataSet() {
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

}