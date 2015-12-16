package com.gpjpe;

public enum ConfigParams {
    KAFKA_TOPIC("KafkaTopic");

    private String name;

    private ConfigParams(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }
}
