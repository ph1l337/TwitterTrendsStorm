package com.gpjpe;

public enum CONFIG {
    KAFKA_TOPIC("KafkaTopic");

    private String name;

    private CONFIG(String name){
        this.name = name;
    }

    public String getName(){
        return this.name;
    }
}
