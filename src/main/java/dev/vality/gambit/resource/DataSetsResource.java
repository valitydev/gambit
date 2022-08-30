package dev.vality.gambit.resource;

import dev.vality.gambit.DataSetNotFound;
import dev.vality.gambit.exception.DataSetInfoAlreadyExistException;
import dev.vality.gambit.service.DataSetService;
import dev.vality.openapi.gambit.api.DataSetsApi;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@RequiredArgsConstructor
@RestController
public class DataSetsResource implements DataSetsApi {

    private final DataSetService dataSetService;

    @Override
    public ResponseEntity<Void> createDataSet(
            @NotNull @Valid String dataSetName,
            @NotNull @Valid MultipartFile file
    ) {
        log.info("createDataSet request: dataSetName {}, file {}", dataSetName, file.getName());
        try {
            dataSetService.createDataSet(dataSetName, file);
        } catch (IllegalArgumentException | DataSetInfoAlreadyExistException e) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Created data set: {}", dataSetName);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @Override
    public ResponseEntity<Void> updateDataSet(
            @NotNull @Valid String dataSetName,
            @NotNull @Valid MultipartFile file
    ) {
        log.info("updateDataSet request: dataSetName {}, file {}", dataSetName, file.getName());
        try {
            dataSetService.updateDataSet(dataSetName, file);
        } catch (IllegalArgumentException | DataSetNotFound e) {
            return ResponseEntity.badRequest().build();
        }
        log.info("Updated data set: {}", dataSetName);
        return ResponseEntity.ok(null);
    }
}
