package dev.vality.gambit.resource;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.service.DataSetService;
import dev.vality.openapi.gambit.api.DataSetsApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import javax.validation.Valid;

@Slf4j
@RequiredArgsConstructor
@RestController
public class DataSetsResource implements DataSetsApi {

    private final DataSetService dataSetService;

    @Override
    public ResponseEntity<Void> createDataSet(@Valid String dataSetName, @Valid MultipartFile file) {
        try {
            validateRequest(dataSetName, file);
            log.info("createDataSet request: dataSetName {}, file {}", dataSetName, file.getOriginalFilename());
            dataSetService.createDataSet(dataSetName.toLowerCase(), file);
        } catch (IllegalArgumentException | DataSetInfoAlreadyExistException e) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Created data set: {}", dataSetName);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @Override
    public ResponseEntity<Void> updateDataSet(@Valid String dataSetName, @Valid MultipartFile file) {
        try {
            validateRequest(dataSetName, file);
            log.info("updateDataSet request: dataSetName {}, file {}", dataSetName, file.getOriginalFilename());
            dataSetService.updateDataSet(dataSetName.toLowerCase(), file);
        } catch (IllegalArgumentException | DataSetNotFound e) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Updated data set: {}", dataSetName);
        return ResponseEntity.ok(null);
    }

    private void validateRequest(String dataSetName, MultipartFile file) {
        if (!StringUtils.hasText(dataSetName) || file == null || file.isEmpty()) {
            log.error("Invalid request. dataSetName {}, file {}", dataSetName, file);
            throw new IllegalArgumentException();
        }
    }
}
