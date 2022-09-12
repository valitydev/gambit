package dev.vality.gambit.factory;

import dev.vality.gambit.DataSetRequest;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BufferedReaderFactory {

    public static BufferedReader create(DataSetRequest dataSetRequest) {
        return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(dataSetRequest.getFile().getCsv())));
    }

    public static BufferedReader create(MultipartFile file) throws IOException {
        return new BufferedReader(new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8));
    }

}
