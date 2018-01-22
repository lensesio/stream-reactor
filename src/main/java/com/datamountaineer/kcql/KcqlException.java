package com.datamountaineer.kcql;


public class KcqlException extends RuntimeException {
    public KcqlException(){
        super();
    }

    public KcqlException(String message){
        super(message);
    }

    public KcqlException(String message, Throwable inner){
        super(message, inner);
    }

    public KcqlException(Throwable inner){
        super(inner);
    }
}
