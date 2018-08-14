package ru.ikss.jiratask.project;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.IssueRestClient.Expandos;
import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.ChangelogGroup;
import com.atlassian.jira.rest.client.api.domain.ChangelogItem;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.IssueLink;
import com.atlassian.jira.rest.client.api.domain.IssueLinkType;
import com.atlassian.jira.rest.client.api.domain.User;
import com.atlassian.jira.rest.client.api.domain.Worklog;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;
import ru.ikss.jiratask.jira.IssueHelper;
import ru.ikss.jiratask.jira.JiraClient;

public class Set10Project extends Project {

    private static final Logger log = LoggerFactory.getLogger(Set10Project.class);
    private static final String INSERT_DATA = "select set10TaskInsert(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_DATA = "select set10TaskUpdate(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String INSERT_WORKLOG = "select set10WorkLogInsert(?, ?, ?, ?, ?)";
    private static final String GET_TIME = "select set10GetLastTaskDate()";
    private static final String JQL = Config.getInstance().getValue("jira.jqlSet10");
    private static final List<String> TEAMS = Arrays.asList("setretaila", "setretailb", "setretaile", "sco");

    @Override
    public void handleTasks() {
        log.trace("Handle project {}", this.getClass().getSimpleName());
        DateTime lastTime = getLastTime();
        String jql = JQL.replace("%dbUpdateTime%", "\'" + lastTime.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm")) + "\'");
        log.trace("JIRA query: " + jql);
        try (JiraRestClient client = JiraClient.getInstance().getClient();
                Connection con = DAO.I.getConnection();
                CallableStatement st = con.prepareCall(INSERT_DATA);
                CallableStatement updateStatement = con.prepareCall(UPDATE_DATA);
                CallableStatement worklogStatement = con.prepareCall(INSERT_WORKLOG)) {
            for (String key : getAllTasks(client, jql)) {
                Issue issue = client.getIssueClient().getIssue(key, Collections.singletonList(Expandos.CHANGELOG)).claim();
                log.trace(issue.getKey() + "\t" + issue.getStatus().getName());
                String team = "";

                User assignee = issue.getAssignee();
                if (assignee != null) {
                    User changelog = client.getUserClient().getUser(assignee.getName()).claim();
                    if ((changelog != null) && (changelog.getGroups() != null) && (changelog.getGroups().getItems() != null)) {
                        for (String cg : changelog.getGroups().getItems()) {
                            if (TEAMS.contains(cg.toLowerCase())) {
                                team = cg;
                                break;
                            }
                        }
                    }
                }
                String caused = "";
                for (IssueLink link : issue.getIssueLinks()) {
                    if (link.getIssueLinkType() != null && IssueLinkType.Direction.INBOUND == link.getIssueLinkType().getDirection() &&
                        "is caused by".equalsIgnoreCase(link.getIssueLinkType().getDescription())) {
                        caused = link.getTargetIssueKey();
                    }
                }
                for (ChangelogGroup cg : issue.getChangelog()) {
                    if (cg.getCreated().compareTo(lastTime) > 0) {
                        for (ChangelogItem ci : cg.getItems()) {
                            if (ci.getField().equals("status")) {
                                insertStatus(st, issue, cg, ci, team, caused);
                            }
                        }
                    }
                }
                insertWorklog(worklogStatement, issue, lastTime);
                updateTask(updateStatement, issue, team, caused);
            }
        } catch (SQLException | IOException e) {
            log.error("Error on handling project", e);
        }
    }

    private static void insertStatus(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci, String team, String caused)
        throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, team);
        st.setInt(7, IssueHelper.getIntFromField(issue, "aggregatetimeoriginalestimate"));
        st.setInt(8, IssueHelper.getIntFromField(issue, "aggregatetimespent"));
        st.setString(9, IssueHelper.getFixVersions(issue));
        st.setString(10, caused);
        st.setString(11, IssueHelper.getValueFromFieldByKey(issue, "parent", "key"));
        st.setString(12, IssueHelper.getValueFromFieldByKey(issue, "creator", "displayName"));
        st.setString(13, issue.getPriority().getName());
        st.setString(14, issue.getSummary());
        st.setString(15, IssueHelper.getValueFromFieldByKey(issue, "customfield_13500", "value")); // IssueRootCause
        st.setString(16, IssueHelper.getStringFromFieldArray(issue, "customfield_10401")); // Sprint
        st.setFloat(17, IssueHelper.getDoubleFromField(issue, "customfield_10105").floatValue()); // StoryPoints
        if (issue.getResolution() != null) {
            st.setString(18, issue.getResolution().getName());
        } else {
            st.setNull(18, Types.VARCHAR);
        }
        st.setString(19, IssueHelper.getValueFromFieldByKey(issue, "creator", "emailAddress"));
        if (issue.getAssignee() != null) {
            st.setString(20, issue.getAssignee().getEmailAddress());
        } else {
            st.setNull(20, Types.VARCHAR);
        }

        String s = IssueHelper.getFieldValueAsString(issue, "customfield_13904");
        if (s != null && !s.isEmpty()) {
            st.setString(21, s);
        } else {
            st.setNull(21, Types.VARCHAR);
        }
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void updateTask(CallableStatement st, Issue issue, String team, String caused) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getStatus().getName());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, team);
        st.setString(5, IssueHelper.getFixVersions(issue));
        st.setString(6, caused);
        st.setString(7, IssueHelper.getValueFromFieldByKey(issue, "parent", "key"));
        st.setString(8, issue.getPriority().getName());
        st.setString(9, IssueHelper.getValueFromFieldByKey(issue, "customfield_13500", "value")); // IssueRootCause
        st.setString(10, IssueHelper.getStringFromFieldArray(issue, "customfield_10401")); // Sprint
        st.setFloat(11, IssueHelper.getDoubleFromField(issue, "customfield_10105").floatValue()); // StoryPoints
        if (issue.getResolution() != null) {
            st.setString(12, issue.getResolution().getName());
        } else {
            st.setNull(12, Types.VARCHAR);
        }
        st.setString(13, IssueHelper.getValueFromFieldByKey(issue, "creator", "emailAddress"));
        if (issue.getAssignee() != null) {
            st.setString(14, issue.getAssignee().getEmailAddress());
        } else {
            st.setNull(14, Types.VARCHAR);
        }
        String s = IssueHelper.getFieldValueAsString(issue, "customfield_13904");
        if (s != null && !s.isEmpty()) {
            st.setString(15, s);
        } else {
            st.setNull(15, Types.VARCHAR);
        }
        Integer originalEstimate = IssueHelper.getIntFromField(issue, "aggregatetimeoriginalestimate");
        if (originalEstimate != null) {
            st.setInt(16, originalEstimate);
        } else {
            st.setNull(16, Types.INTEGER);
        }
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void insertWorklog(CallableStatement st, Issue issue, DateTime lasttime) throws SQLException {
        for (Worklog wl : issue.getWorklogs()) {
            if (wl.getUpdateDate().compareTo(lasttime) > 0) {
                st.setString(1, issue.getKey());
                st.setString(2, wl.getUpdateAuthor().getDisplayName());
                st.setInt(3, wl.getMinutesSpent());
                st.setTimestamp(4, new Timestamp(wl.getUpdateDate().getMillis()));
                st.setTimestamp(5, new Timestamp(wl.getStartDate().getMillis()));
                log.debug("query: '{}'", st.toString());
                st.execute();
            }
        }
    }

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }

    public static void main(String[] args) {
        new Set10Project().handleTasks();
    }
}
