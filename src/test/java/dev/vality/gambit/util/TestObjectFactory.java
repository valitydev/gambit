package dev.vality.gambit.util;

import dev.vality.gambit.domain.tables.pojos.Data;
import dev.vality.gambit.domain.tables.pojos.DataLookup;
import dev.vality.gambit.domain.tables.pojos.DataSetInfo;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestObjectFactory {

    public static final String DATA_SET_INFO_NAME = "alpha_numeric_data_set";
    public static final String DATA_SET_INFO_HEADERS = "headerOne,headerTwo,headerThree";
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
            dataList.add(new Data((long) i, dataSetInfoId, i + ",b,c", i + "_hash"));
        }
        return dataList;
    }

    public static Data createData(Integer dataSetInfoId, String values, String valuesHash) {
        var data = new Data();
        data.setDataSetInfoId(dataSetInfoId);
        data.setValues(values);
        data.setValuesHash(valuesHash);
        return data;
    }

}
