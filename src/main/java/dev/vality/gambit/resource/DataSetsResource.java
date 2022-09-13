package dev.vality.gambit.resource;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.exception.FileProcessingException;
import dev.vality.gambit.service.DataSetService;
import dev.vality.gambit.factory.BufferedReaderFactory;
import dev.vality.openapi.gambit.api.DataSetsApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;
import java.io.BufferedReader;
import java.io.IOException;

@Slf4j
@RequiredArgsConstructor
@RestController
public class DataSetsResource implements DataSetsApi {

    private final DataSetService dataSetService;

    private static final String FILE_TYPE = "text/csv";

    @Override
    public ResponseEntity<Void> createDataSet(@Valid String dataSetName, @Valid MultipartFile file) {
        try (BufferedReader bufferedReader = BufferedReaderFactory.create(file)) {
            log.info("createDataSet request: dataSetName {}, file {}", dataSetName, file.getOriginalFilename());
            validateFileType(file);
            dataSetService.createDataSet(dataSetName.toLowerCase(), bufferedReader);
        } catch (IllegalArgumentException | DataSetInfoAlreadyExistException e) {
            return ResponseEntity.badRequest().build();
        } catch (IOException e) {
            log.error("Error during file processing, file: {}", file.getOriginalFilename());
            throw new FileProcessingException(e);
        }
        log.info("Created data set: {}", dataSetName);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @Override
    public ResponseEntity<Void> updateDataSet(@Valid String dataSetName, @Valid MultipartFile file) {
        try (BufferedReader bufferedReader = BufferedReaderFactory.create(file)) {
            log.info("updateDataSet request: dataSetName {}, file {}", dataSetName, file.getOriginalFilename());
            validateFileType(file);
            dataSetService.updateDataSet(dataSetName.toLowerCase(), bufferedReader);
        } catch (IllegalArgumentException | DataSetNotFound e) {
            return ResponseEntity.badRequest().build();
        } catch (IOException e) {
            log.error("Error during file processing, file: {}", file.getOriginalFilename());
            throw new FileProcessingException(e);
        }
        log.info("Updated data set: {}", dataSetName);
        return ResponseEntity.ok(null);
    }

    private void validateFileType(MultipartFile file) {
        if (!FILE_TYPE.equals(file.getContentType())) {
            log.error("File {} has incorrect content type {}", file.getOriginalFilename(), file.getContentType());
            throw new IllegalArgumentException();
        }
    }

}
