package com.harold.starter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

public class Main {
    private static final String config_name= "config.properties";
    public static void main(String[] args) {
        Properties props = new Properties();
        try{
            ClassLoader classLoader = ProducerStarter.class.getClassLoader();
            URL res = Objects.requireNonNull(classLoader.getResource(config_name), "Can't find configuration file app.config");
            InputStream inputStream = new FileInputStream(res.getFile());
            props.load(inputStream);
        }catch (IOException e){
            e.printStackTrace();
        }
        ProducerStarter.startProducer(props);
        ConsumerStarter.startConsumer(props);
    }
}
