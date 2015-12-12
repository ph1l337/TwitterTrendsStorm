package com.gpjpe;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final static String CONFIG_FILE = "config.properties";
    private final static Logger LOGGER = Logger.getLogger(AppConfig.class.getName());

    Properties properties;

    public AppConfig() {
        InputStream inputStream;

        properties = new Properties();

        inputStream = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE);

        try {
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                throw new FileNotFoundException("Property file '" + CONFIG_FILE + "' not found in the classpath");
            }
        } catch (IOException e) {
            LOGGER.error(e);
            throw new RuntimeException(e);
        }
    }

    public String getProperty(CONFIG key, String defaultValue){
        return this.properties.getProperty(key.getName(), defaultValue);
    }
}
