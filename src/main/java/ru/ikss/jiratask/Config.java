package ru.ikss.jiratask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private static Config instance;
    private Properties props;

    private Config() {
        props = new Properties();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("config/config.conf")), "UTF-8"))) {
            props.load(br);
        } catch (IOException e) {
            log.error("Error while loading properties", e);
            System.exit(-1);
        }
    }

    public static Config getInstance() {
        if (instance == null)
            instance = new Config();
        return instance;
    }

    public String getValue(String propKey) {
        return this.props.getProperty(propKey);
    }

    public String getValue(String propKey, String defaultValue) {
        return this.props.getProperty(propKey, defaultValue);
    }
}
