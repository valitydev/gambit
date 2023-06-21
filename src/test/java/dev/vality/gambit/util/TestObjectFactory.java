package dev.vality.gambit.util;

import dev.vality.gambit.DataSetRequest;
import dev.vality.gambit.File;
import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestObjectFactory {

    public static final String DATA_SET_INFO_NAME = "alpha_numeric_data_set";
    public static final String DATA_SET_INFO_HEADERS = "h1,h2,h3";
    public static final Integer DATA_SET_INFO_ID = 55;

    public static final String DATA_SET_INFO_IP_NAME = "alpha_numeric_ip_data_set";
    public static final String DATA_SET_INFO_IP_HEADERS = "ip";
    public static final Integer DATA_SET_INFO_IP_ID = 29;


    public static List<DataSetInfo> createDataSetInfoList(int count) {
        List<DataSetInfo> dataSetInfos = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            dataSetInfos.add(new DataSetInfo(i, String.valueOf(i), String.valueOf(i)));
        }
        return dataSetInfos;
    }

    public static DataSetInfo createDefaultDataSetInfo() {
        return new DataSetInfo(DATA_SET_INFO_ID, DATA_SET_INFO_NAME, DATA_SET_INFO_HEADERS);
    }

    public static List<DataLookup> createDataLookupList(int hash, int count) {
        List<DataLookup> dataLookups = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            dataLookups.add(new DataLookup((long) i, i, (long) i, hash));
        }
        return dataLookups;
    }

    public static List<Data> createDataList(Integer dataSetInfoId, int count) {
        List<Data> dataList = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            dataList.add(new Data((long) i, dataSetInfoId, i + ",b,c"));
        }
        return dataList;
    }

    public static Data createData(Integer dataSetInfoId, String values) {
        var data = new Data();
        data.setDataSetInfoId(dataSetInfoId);
        data.setValues(values);
        return data;
    }

    public static DataSetRequest createDataSetRequest(String dataSetName, String fileName) {
        return createDataSetRequest(dataSetName, getBytesFromFile(fileName));
    }

    public static DataSetRequest createDataSetRequest(String dataSetName, byte[] bytes) {
        File file = new File();
        file.setCsv(bytes);
        return new DataSetRequest()
                .setDataSetName(dataSetName)
                .setFile(file);
    }

    @SneakyThrows
    public static byte[] getBytesFromFile(String fileName) {
        return Files.readAllBytes(Path.of("src/test/resources/data_sets/" + fileName));
    }

    @SneakyThrows
    public static BufferedReader createBufferedReader(String fileName) {
        return new BufferedReader(new InputStreamReader(
                Files.newInputStream(Path.of("src/test/resources/data_sets/" + fileName))));
    }

    public static MultipartFile createMultipartFile(String name) {
        return createMultipartFile(name, "text/csv");
    }

    @SneakyThrows
    public static MultipartFile createMultipartFile(String name, String contentType) {
        return new MockMultipartFile(
                name,
                null,
                contentType,
                Files.newInputStream(Path.of("src/test/resources/data_sets/" + name))
        );
    }


    public static String randomString() {
        return UUID.randomUUID().toString();
    }
}
