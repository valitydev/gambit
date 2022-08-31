package dev.vality.gambit.exception;

public class DataSetInfoAlreadyExistException extends RuntimeException {

    public DataSetInfoAlreadyExistException(String dataSetName) {
        super(String.format("Data set with name %s already exists.", dataSetName));
    }
}
