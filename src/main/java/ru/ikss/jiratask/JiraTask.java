package ru.ikss.jiratask;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.IssueRestClient.Expandos;
import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.ChangelogGroup;
import com.atlassian.jira.rest.client.api.domain.ChangelogItem;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.IssueField;
import com.atlassian.jira.rest.client.api.domain.SearchResult;
import com.atlassian.jira.rest.client.api.domain.User;
import com.atlassian.jira.rest.client.api.domain.Version;
import com.atlassian.jira.rest.client.api.domain.Worklog;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.google.common.collect.Iterables;

public class JiraTask {

    private static enum Projects {
        SET5, SET10, CR;
    }
    private static final Logger logger = LoggerFactory.getLogger(JiraTask.class);
    private static final int maxResults = 500;
    private static Properties props;
    private static final Set<String> fields =
            Stream.of("summary", "issuetype", "created", "updated", "project", "status", "key").collect(Collectors.toSet());
    private static final URI JIRA_SERVER_URI = URI.create("https://crystals.atlassian.net");
    private static final AsynchronousJiraRestClientFactory factory = new AsynchronousJiraRestClientFactory();

    public static void main(String[] args) throws IOException {
        props = new Properties();
        try (FileReader reader = new FileReader(new File("config/config.conf"))) {
            props.load(reader);
        }
        Integer delay = Integer.valueOf(props.getProperty("delay", "60"));
        logger.trace("------ Work started ------");
        logger.debug("Execute with delay = {}", delay);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleWithFixedDelay(JiraTask::handleProjects, 0, delay, TimeUnit.MINUTES);
    }

    private static void handleProjects() {
        logger.trace("------ Start handling project ------");
        try (Connection con = DB.getConnection(props);
                JiraRestClient restClient =
                        factory.createWithBasicHttpAuthentication(JIRA_SERVER_URI, props.getProperty("jira.login"), props.getProperty("jira.pwd"))) {
            for (Projects project : Projects.values()) {
                logger.trace("Handle project {}", project);
                handleProject(con, restClient, project);
                logger.trace("End");
                logger.trace("\n");
            }
            try (CallableStatement st = con.prepareCall(DB.queryRecalcData)) {
                logger.trace("Start recalc");
                st.execute();
            }
        } catch (Exception ex) {
            logger.error("Exception", ex);
        }
        logger.trace("------ All projects handled ------");
    }

    private static void handleProject(Connection con, JiraRestClient restClient, Projects project) throws SQLException {
        int startAt = 0;
        String jql = "";
        String query = "";
        DateTime lastTime;
        List<String> set10Teams = Stream.of(props.getProperty("jira.set10Teams").split(","))
            .peek(String::trim)
            .collect(Collectors.toList());
        switch (project) {
            case SET5:
                lastTime = getLastTime(con, DB.queryGetTimeSet5);
                jql = props.getProperty("jira.jqlSet5");
                query = DB.queryInsertDataSet5;
                break;
            case SET10:
                lastTime = getLastTime(con, DB.queryGetTimeSet10);
                jql = props.getProperty("jira.jqlSet10");
                query = DB.queryInsertDataSet10;
                break;
            case CR:
                lastTime = getLastTime(con, DB.queryGetTimeCR);
                jql = props.getProperty("jira.jqlCR");
                query = DB.queryInsertDataCR;
                break;
            default:
                return;
        }
        jql = jql.replace("%dbUpdateTime%", "\'" + lastTime.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm")) + "\'");
        logger.trace("JIRA query: " + jql);
        SearchResult searchJqlPromise = restClient.getSearchClient().searchJql(jql, maxResults, startAt, fields).claim();
        logger.trace("Total {} works: {}", project, Integer.valueOf(searchJqlPromise.getTotal()));
        try (CallableStatement st = con.prepareCall(query)) {
            boolean hasData = true;
            while (hasData) {
                hasData = false;
                for (Issue issue : searchJqlPromise.getIssues()) {
                    hasData = true;
                    logger.trace(issue.getKey() + "\t" + issue.getStatus().getName());
                    Issue issueTotal =
                            restClient.getIssueClient().getIssue(issue.getKey(), Collections.singletonList(Expandos.CHANGELOG)).claim();
                    String team = "";
                    if (project == Projects.SET10) {
                        User assignee = issueTotal.getAssignee();
                        if (assignee != null) {
                            User changelog = restClient.getUserClient().getUser(assignee.getName()).claim();
                            if ((changelog != null) && (changelog.getGroups() != null) && (changelog.getGroups().getItems() != null)) {
                                for (String cg : changelog.getGroups().getItems()) {
                                    if (set10Teams.contains(cg.toLowerCase())) {
                                        team = cg;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    for (ChangelogGroup cg : issueTotal.getChangelog()) {
                        if (cg.getCreated().compareTo(lastTime) > 0) {
                            for (ChangelogItem ci : cg.getItems()) {
                                if (ci.getField().equals("status")) {
                                    switch (project) {
                                        case SET5:
                                            insertStatusSet5(st, issueTotal, cg, ci);
                                            break;
                                        case SET10:
                                            insertStatusSet10(st, issueTotal, cg, ci, team);
                                            break;
                                        case CR:
                                            insertStatusCR(st, issueTotal, cg, ci);
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    if (project == Projects.CR) {
                        insertWorklogCR(issueTotal, lastTime);
                        updateTaskCR(issueTotal);
                        if (issueTotal.getCreationDate().compareTo(lastTime) > 0) {
                            insertCreateCR(st, issueTotal);
                        }
                    }
                }
                if (hasData) {
                    startAt += Iterables.size(searchJqlPromise.getIssues());
                    searchJqlPromise =
                            restClient.getSearchClient().searchJql(jql, maxResults, startAt, fields).claim();
                }
            }
        }
    }

    private static void updateTaskCR(Issue issue) throws SQLException {
        try (Connection con = DB.getConnection(props); CallableStatement st = con.prepareCall(DB.queryUpdateDataCR)) {
            st.setString(1, issue.getKey());
            st.setString(2, issue.getStatus().getName());
            st.setString(3, getFixVersion(issue));
            logger.debug("query: '{}'", st.toString());
            st.execute();
        }
    }

    private static void insertWorklogCR(Issue issue, DateTime lasttime) throws SQLException {
        try (Connection con = DB.getConnection(props); CallableStatement st = con.prepareCall(DB.queryInsertWorklogCR)) {
            for (Worklog wl : issue.getWorklogs()) {
                if (wl.getUpdateDate().compareTo(lasttime) > 0) {
                    st.setString(1, issue.getKey());
                    st.setString(2, wl.getUpdateAuthor().getDisplayName());
                    st.setInt(3, wl.getMinutesSpent());
                    st.setTimestamp(4, new Timestamp(wl.getUpdateDate().getMillis()));
                    logger.debug("query: '{}'", st.toString());
                    st.execute();
                }
            }
        }
    }

    private static DateTime getLastTime(Connection con, String query) throws SQLException {
        DateTime lastTime = new DateTime(0);
        try (Statement st = con.createStatement(); ResultSet rs = st.executeQuery(query)) {
            if (rs.next())
                lastTime = new DateTime(rs.getTimestamp(1));
        }
        return lastTime;
    }

    private static void insertStatusSet5(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setInt(6, getDoubleFromField(issue, props.getProperty("jira.field.devtime")).intValue());
        st.setInt(7, getDoubleFromField(issue, props.getProperty("jira.field.testtime")).intValue());
        st.setString(8, getFixVersion(issue));
        logger.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void insertStatusSet10(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci, String team) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, team);
        st.setInt(7, getIntFromField(issue, "aggregatetimeoriginalestimate").intValue());
        st.setInt(8, getIntFromField(issue, "aggregatetimespent").intValue());
        st.setString(9, getFixVersion(issue));
        logger.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void insertCreateCR(CallableStatement st, Issue issue) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, null);
        st.setString(5, issue.getStatus().getName());
        st.setString(6, issue.getAssignee() == null ? null : issue.getAssignee().getDisplayName());
        st.setString(7, getValueFromFieldByKey(issue, "creator", "displayName"));
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, getFixVersion(issue));
        st.setInt(10, getIntFromField(issue, "aggregatetimeoriginalestimate"));
        st.setInt(11, getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, getStringFromFieldArray(issue, "customfield_12601", ","));
        st.setString(15, getValueFromFieldByKey(issue, "customfield_12606", "displayName"));
        st.setString(16, issue.getReporter().getDisplayName());
        st.setTimestamp(17, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(18, getStringFromFieldArray(issue, "customfield_12800", ","));
        logger.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static String getValueFromFieldByKey(Issue issue, String fieldName, String key) {
        try {
            IssueField field = issue.getField(fieldName);
            if (field != null && field.getValue() != null) {
                JSONObject value = (JSONObject) field.getValue();
                return value.getString(key);
            }
        } catch (Exception e) {
            logger.error("Can\'t get {}", fieldName, e);
        }
        return "";
    }

    private static void insertStatusCR(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, issue.getAssignee() == null ? null : issue.getAssignee().getDisplayName());
        st.setString(7, getValueFromFieldByKey(issue, "creator", "displayName"));
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, getFixVersion(issue));
        st.setInt(10, getIntFromField(issue, "aggregatetimeoriginalestimate").intValue());
        st.setInt(11, getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, getStringFromFieldArray(issue, "customfield_12601", ","));
        st.setString(15, getValueFromFieldByKey(issue, "customfield_12606", "displayName"));
        st.setString(16, cg.getAuthor().getDisplayName());
        st.setTimestamp(17, new Timestamp(cg.getCreated().getMillis()));
        st.setString(18, getStringFromFieldArray(issue, "customfield_12800", ","));
        logger.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static String getStringFromFieldArray(Issue issue, String fieldName, String delimeter) {
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
            logger.error("Can\'t get {}", fieldName, e);
        }
        return result;
    }

    private static String getFixVersion(Issue issue) {
        String result = "";
        Iterable<Version> iterator = issue.getFixVersions();
        if (iterator != null) {
            result = StreamSupport.stream(iterator.spliterator(), false)
                .map(Version::getName)
                .collect(Collectors.joining(","));
        }
        return result;
    }

    private static Integer getIntFromField(Issue issue, String key) {
        Integer result = Integer.valueOf(0);
        IssueField field = issue.getField(key);
        if (field != null && field.getValue() != null) {
            result = (Integer) field.getValue();
        }
        return result;
    }

    private static Double getDoubleFromField(Issue issue, String key) {
        Double result = Double.valueOf(0.0D);
        IssueField field = issue.getField(key);
        if (field != null && field.getValue() != null) {
            result = (Double) field.getValue();
        }
        return result;
    }

}
