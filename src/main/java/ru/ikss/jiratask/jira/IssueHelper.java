package ru.ikss.jiratask.jira;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.IssueField;
import com.atlassian.jira.rest.client.api.domain.Version;

public class IssueHelper {

    private static final Logger log = LoggerFactory.getLogger(IssueHelper.class);

    public static String getStringFromFieldArray(Issue issue, String fieldName) {
        String result = "";
        try {
            IssueField field = issue.getField(fieldName);
            if (field != null && field.getValue() != null) {
                JSONArray value = (JSONArray) field.getValue();
                for (int i = 0; i < value.length(); ++i) {
                    if (!result.isEmpty()) {
                        result = result + ",";
                    }
                    result = result + (String) value.get(i);
                }
            }
        } catch (Exception e) {
            log.error("Can\'t get {}", fieldName, e);
        }
        return result;
    }

    public static String getFixVersions(Issue issue) {
        String result = "";
        Iterable<Version> iterator = issue.getFixVersions();
        if (iterator != null) {
            result = StreamSupport.stream(iterator.spliterator(), false)
                .map(Version::getName)
                .collect(Collectors.joining(","));
        }
        return result;
    }

    public static Integer getIntFromField(Issue issue, String key) {
        Integer result = Integer.valueOf(0);
        IssueField field = issue.getField(key);
        if (field != null && field.getValue() != null) {
            result = (Integer) field.getValue();
        }
        return result;
    }

    public static Double getDoubleFromField(Issue issue, String key) {
        Double result = Double.valueOf(0.0D);
        IssueField field = issue.getField(key);
        if (field != null && field.getValue() != null) {
            result = (Double) field.getValue();
        }
        return result;
    }

    public static String getValueFromFieldByKey(Issue issue, String fieldName, String key) {
        try {
            IssueField field = issue.getField(fieldName);
            if (field != null && field.getValue() != null) {
                JSONObject value = (JSONObject) field.getValue();
                return value.getString(key);
            }
        } catch (Exception e) {
            log.error("Can\'t get {}", fieldName, e);
        }
        return "";
    }

}
