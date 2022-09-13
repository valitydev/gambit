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

import java.io.BufferedReader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
                .createDataSet(anyString(), any(BufferedReader.class));
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void createDataSetWrongContentType() {
        MultipartFile file = TestObjectFactory.createMultipartFile("create.csv", "application/json");
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void createDataSetDataSetInfoAlreadyExistException() {
        doThrow(new DataSetInfoAlreadyExistException(TestObjectFactory.DATA_SET_INFO_NAME))
                .when(dataSetService)
                .createDataSet(anyString(), any(BufferedReader.class));
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
                .updateDataSet(anyString(), any(BufferedReader.class));
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateDataSetDataSetInfoAlreadyExistException() throws DataSetNotFound {
        doThrow(new DataSetNotFound())
                .when(dataSetService)
                .updateDataSet(anyString(), any(BufferedReader.class));
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateDataSetWrongContentType() {
        MultipartFile file = TestObjectFactory.createMultipartFile("update.csv", "application/json");
        ResponseEntity<Void> response = resource.createDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    void updateDataSet() {
        ResponseEntity<Void> response = resource.updateDataSet(TestObjectFactory.DATA_SET_INFO_NAME, file);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

}