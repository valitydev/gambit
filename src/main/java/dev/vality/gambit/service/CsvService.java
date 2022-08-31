package dev.vality.gambit.service;

import dev.vality.gambit.model.DataEntries;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface CsvService {

    DataEntries process(MultipartFile file);

    DataEntries process(MultipartFile file, List<String> existingHeaders);

}
