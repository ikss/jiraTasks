package ru.ikss.jiratask;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
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
    private static final List<String> set10Teams = Arrays.asList("setretaila", "setretailb", "setretaile");
    private static final Set<String> fields =
            Stream.of("summary", "issuetype", "created", "updated", "project", "status", "key").collect(Collectors.toSet());

    public static void main(String[] args) throws IOException {
        props = new Properties();
        try (FileReader reader = new FileReader(new File("config/config.conf"))) {
            props.load(reader);
        }
        final URI jiraServerUri = URI.create("https://crystals.atlassian.net");
        final AsynchronousJiraRestClientFactory factory = new AsynchronousJiraRestClientFactory();
        try (Connection con = DB.getConnection(props);
                JiraRestClient restClient =
                        factory.createWithBasicHttpAuthentication(jiraServerUri, props.getProperty("jira.login"), props.getProperty("jira.pwd"))) {
            for (Projects project : Projects.values()) {
                try {
                    handleProject(con, restClient, project);
                } catch (SQLException e) {
                    logger.error("Error on handle project {}", project, e);
                }
            }
            try (CallableStatement st = con.prepareCall(DB.queryRecalcData)) {
                logger.trace("Start recalc");
                st.execute();
            }
        } catch (Exception ex) {
            logger.error("Exception: " + ex.getCause() + " " + ex.getMessage());
        }
        logger.trace("All done");
    }

    private static void handleProject(Connection con, JiraRestClient restClient, Projects project) throws SQLException {
        logger.trace("Handle project {}", project);
        int startAt = 0;
        String jql = "";
        String query = "";
        DateTime lastTime;
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
                        User changelog = restClient.getUserClient().getUser(issueTotal.getAssignee().getName()).claim();
                        for (String cg : changelog.getGroups().getItems()) {
                            if (set10Teams.contains(cg.toLowerCase())) {
                                team = cg;
                                break;
                            }
                        }
                    }
                    for (ChangelogGroup cg : issueTotal.getChangelog()) {
                        for (ChangelogItem ci : cg.getItems()) {
                            if (ci.getField().equals("status") && cg.getCreated().compareTo(lastTime) > 0) {
                                switch (project) {
                                    case SET5:
                                        insertStatusSet5(st, issueTotal, cg, ci);
                                        break;
                                    case SET10:
                                        insertStatusSet10(st, issueTotal, cg, ci, team);
                                        break;
                                    case CR:
                                        insertStatusCR(st, issueTotal, cg, ci);
                                }
                            }
                        }
                    }
                    if (project == Projects.CR) {
                        insertWorklogCR(issueTotal, lastTime);
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

    private static void insertWorklogCR(Issue issue, DateTime lasttime) throws SQLException {
        try (Connection con = DB.getConnection(props); CallableStatement st = con.prepareCall(DB.queryInsertWorklogCR)) {
            for (Worklog wl : issue.getWorklogs()) {
                if (wl.getUpdateDate().compareTo(lasttime) > 0) {
                    st.setString(1, issue.getKey());
                    st.setString(2, wl.getUpdateAuthor().getDisplayName());
                    st.setInt(3, wl.getMinutesSpent());
                    st.setTimestamp(4, new Timestamp(wl.getUpdateDate().getMillis()));
                    st.execute();
                }
            }
        }
    }

    private static DateTime getLastTime(Connection con, String query) {
        DateTime lastTime = new DateTime(0);
        try (CallableStatement st = con.prepareCall(query); ResultSet rs = st.executeQuery()) {
            if (rs.next())
                lastTime = new DateTime(rs.getTimestamp(1));
        } catch (SQLException e1) {
            e1.printStackTrace();
            System.exit(0);
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
        st.execute();
    }

    private static void insertCreateCR(CallableStatement st, Issue issue) throws SQLException {
        String creator = "";
        IssueField creatorField = issue.getField("creator");
        if (creatorField != null && creatorField.getValue() != null) {
            JSONObject project = (JSONObject) creatorField.getValue();
            try {
                creator = project.getString("displayName");
            } catch (JSONException var14) {
                logger.error("Can\'t get creator");
            }
        }

        String pr = "";
        IssueField projectField = issue.getField("customfield_12601");
        if (projectField != null && projectField.getValue() != null) {
            JSONArray projects = (JSONArray) projectField.getValue();

            for (int i = 0; i < projects.length(); ++i) {
                if (!pr.isEmpty()) {
                    pr = pr + ",";
                }

                try {
                    pr = pr + (String) projects.get(i);
                } catch (JSONException e) {
                    logger.error("Can\'t get project", e);
                }
            }
        }

        String pmInside = "";
        IssueField pmInsideField = issue.getField("customfield_12606");
        if (pmInsideField != null && pmInsideField.getValue() != null) {
            try {
                pmInside = ((JSONObject) pmInsideField.getValue()).getString("displayName");
            } catch (JSONException var12) {
                logger.error("Can\'t get pmInside", var12);
            }
        }

        String team = "";
        IssueField teamField = issue.getField("customfield_12800");
        if (teamField != null && teamField.getValue() != null) {
            JSONArray teams = (JSONArray) teamField.getValue();

            for (int i = 0; i < teams.length(); ++i) {
                if (!team.isEmpty()) {
                    team = team + ",";
                }

                try {
                    team = team + (String) teams.get(i);
                } catch (JSONException e) {
                    logger.error("Can\'t get teams", e);
                }
            }
        }

        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, null);
        st.setString(5, issue.getStatus().getName());
        st.setString(6, issue.getAssignee().getDisplayName());
        st.setString(7, creator);
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, getFixVersion(issue));
        st.setInt(10, getIntFromField(issue, "aggregatetimeoriginalestimate").intValue());
        st.setInt(11, getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, pr);
        st.setString(15, pmInside);
        st.setString(16, issue.getReporter().getDisplayName());
        st.setTimestamp(17, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(18, team);
        st.execute();
    }

    private static void insertStatusCR(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        String creator = "";
        IssueField creatorField = issue.getField("creator");
        if (creatorField != null && creatorField.getValue() != null) {
            JSONObject project = (JSONObject) creatorField.getValue();
            try {
                creator = project.getString("displayName");
            } catch (JSONException var14) {
                logger.error("Can\'t get creator");
            }
        }

        String pr = "";
        IssueField projectField = issue.getField("customfield_12601");
        if (projectField != null && projectField.getValue() != null) {
            JSONArray projects = (JSONArray) projectField.getValue();

            for (int i = 0; i < projects.length(); ++i) {
                if (!pr.isEmpty()) {
                    pr = pr + ",";
                }

                try {
                    pr = pr + (String) projects.get(i);
                } catch (JSONException e) {
                    logger.error("Can\'t get project", e);
                }
            }
        }

        String pmInside = "";
        IssueField pmInsideField = issue.getField("customfield_12606");
        if (pmInsideField != null && pmInsideField.getValue() != null) {
            try {
                pmInside = ((JSONObject) pmInsideField.getValue()).getString("displayName");
            } catch (JSONException var12) {
                logger.error("Can\'t get pmInside", var12);
            }
        }

        String team = "";
        IssueField teamField = issue.getField("customfield_12800");
        if (teamField != null && teamField.getValue() != null) {
            JSONArray teams = (JSONArray) teamField.getValue();

            for (int i = 0; i < teams.length(); ++i) {
                if (!team.isEmpty()) {
                    team = team + ",";
                }

                try {
                    team = team + (String) teams.get(i);
                } catch (JSONException e) {
                    logger.error("Can\'t get teams", e);
                }
            }
        }

        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, issue.getAssignee().getDisplayName());
        st.setString(7, creator);
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, getFixVersion(issue));
        st.setInt(10, getIntFromField(issue, "aggregatetimeoriginalestimate").intValue());
        st.setInt(11, getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, pr);
        st.setString(15, pmInside);
        st.setString(16, cg.getAuthor().getDisplayName());
        st.setTimestamp(17, new Timestamp(cg.getCreated().getMillis()));
        st.setString(18, team);
        st.execute();
    }

    private static String getFixVersion(Issue issue) {
        String result = "";
        for (Version version : issue.getFixVersions()) {
            if (!result.isEmpty()) {
                result += ",";
            }
            result = result + version.getName();
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
