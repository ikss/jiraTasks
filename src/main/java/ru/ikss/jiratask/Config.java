package ru.ikss.jiratask;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

    private static final Logger log = LoggerFactory.getLogger(Config.class);
    private static Config instance;
    private Properties props = new Properties();

    public static Config getInstance() {
        if (instance == null)
            instance = new Config();
        return instance;
    }

    private Config() {
        try {
            loadProperties();
        } catch (Exception e) {
            log.error("Error while loading properties " + e.getMessage(), e);
            System.exit(-1);
        }
    }

    private void loadProperties() throws Exception {
        Properties temp = new Properties();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(new FileInputStream(new File("config/config.conf")), StandardCharsets.UTF_8))) {
            temp.load(br);
        }
        setProperties(temp);
    }

    void setProperties(Properties p) {
        props = new Properties();
        p.forEach((k, v) -> props.put(getKey(k.toString()), v.toString().trim()));
    }

    public String getValue(String key) {
        return props.getProperty(getKey(key));
    }

    public String getValue(String key, String defaultValue) {
        return props.getProperty(getKey(key), defaultValue);
    }

    public Integer getInt(String key, Integer defaultValue) {
        String value = props.getProperty(getKey(key));
        return parseInt(value, defaultValue, key);
    }

    public List<String> getList(String key) {
        return Arrays.asList(getValue(key, "").split("\\s*(,|;)\\s*"));
    }

    public List<Integer> getListInt(String key) {
        return Arrays.stream(getValue(key, "")
            .split("\\s*(,|;)\\s*"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> parseInt(s, null, key))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private static Integer parseInt(String string, Integer defaultValue, String paramName) {
        Integer result = defaultValue;
        if (string != null)
            try {
                result = Integer.parseInt(string);
            } catch (NumberFormatException e) {
                log.error("Неправильное значение параметра \"{}\"; используется значение по умолчанию \"{}\"", paramName, defaultValue, e);
            }
        return result;
    }

    private static String getKey(String key) {
        return key.toLowerCase();
    }
}
