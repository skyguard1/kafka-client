package com.skyguard.kafka.client.common;

public enum  ClientType {

    PRODUCER(1,"生产者"),CONSUMER(2,"消费者")
    ;


    private int code;
    private String message;

    ClientType(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }


}
