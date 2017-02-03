package ru.ikss.jiratask.project;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.IssueRestClient.Expandos;
import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.ChangelogGroup;
import com.atlassian.jira.rest.client.api.domain.ChangelogItem;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.Worklog;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;
import ru.ikss.jiratask.jira.IssueHelper;
import ru.ikss.jiratask.jira.JiraClient;

public class CRProject extends Project {

    private static final Logger log = LoggerFactory.getLogger(CRProject.class);
    private static final String INSERT_DATA = "select CRTaskInsert(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_DATA = "select CRTaskUpdate(?, ?, ?, ?)";
    private static final String INSERT_WORKLOG = "select CRWorkLogInsert(?, ?, ?, ?)";
    private static final String JQL = Config.getInstance().getValue("jira.jqlCR");
    private static final String GET_TIME = "select CRGetLastTaskDate()";

    @Override
    public void handleTasks() {
        log.trace("Handle project {}", this.getClass().getSimpleName());
        DateTime lastTime = getLastTime();
        String jql = JQL.replace("%dbUpdateTime%", "\'" + lastTime.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm")) + "\'");
        log.trace("JIRA query: " + jql);
        try (JiraRestClient client = JiraClient.getInstance().getClient();
                Connection con = DAO.I.getConnection();
                CallableStatement insertStatement = con.prepareCall(INSERT_DATA);
                CallableStatement updateStatement = con.prepareCall(UPDATE_DATA);
                CallableStatement worklogStatement = con.prepareCall(INSERT_WORKLOG)) {
            for (String key : getAllTasks(client, jql)) {
                Issue issue = client.getIssueClient().getIssue(key, Collections.singletonList(Expandos.CHANGELOG)).claim();
                log.trace(issue.getKey() + "\t" + issue.getStatus().getName());
                for (ChangelogGroup cg : issue.getChangelog()) {
                    if (cg.getCreated().compareTo(lastTime) > 0) {
                        for (ChangelogItem ci : cg.getItems()) {
                            if (ci.getField().equals("status")) {
                                insertStatus(insertStatement, issue, cg, ci);
                            }
                        }
                    }
                }
                insertWorklog(worklogStatement, issue, lastTime);
                updateTask(updateStatement, issue);
                if (issue.getCreationDate().compareTo(lastTime) > 0) {
                    createTask(insertStatement, issue);
                }
            }
        } catch (SQLException | IOException e) {
            log.error("Error on handling project", e);
        }
        log.trace("End\n");
    }

    private static void updateTask(CallableStatement st, Issue issue) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getStatus().getName());
        st.setString(3, IssueHelper.getFixVersions(issue));
        st.setInt(4, IssueHelper.getDoubleFromField(issue, "customfield_12701").intValue());
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
                log.debug("query: '{}'", st.toString());
                st.execute();
            }
        }
    }

    private static void createTask(CallableStatement st, Issue issue) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, null);
        st.setString(5, issue.getStatus().getName());
        st.setString(6, issue.getAssignee() == null ? null : issue.getAssignee().getDisplayName());
        st.setString(7, IssueHelper.getValueFromFieldByKey(issue, "creator", "displayName"));
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, IssueHelper.getFixVersions(issue));
        st.setInt(10, IssueHelper.getIntFromField(issue, "aggregatetimeoriginalestimate"));
        st.setInt(11, IssueHelper.getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, IssueHelper.getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, IssueHelper.getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, IssueHelper.getStringFromFieldArray(issue, "customfield_12601", ","));
        st.setString(15, IssueHelper.getValueFromFieldByKey(issue, "customfield_12606", "displayName"));
        st.setString(16, issue.getReporter().getDisplayName());
        st.setTimestamp(17, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(18, IssueHelper.getStringFromFieldArray(issue, "customfield_12800", ","));
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void insertStatus(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getSummary());
        st.setString(3, issue.getIssueType().getName());
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, issue.getAssignee() == null ? null : issue.getAssignee().getDisplayName());
        st.setString(7, IssueHelper.getValueFromFieldByKey(issue, "creator", "displayName"));
        st.setTimestamp(8, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(9, IssueHelper.getFixVersions(issue));
        st.setInt(10, IssueHelper.getIntFromField(issue, "aggregatetimeoriginalestimate").intValue());
        st.setInt(11, IssueHelper.getIntFromField(issue, "aggregatetimespent").intValue());
        st.setInt(12, IssueHelper.getDoubleFromField(issue, "customfield_12701").intValue());
        st.setInt(13, IssueHelper.getDoubleFromField(issue, "customfield_12702").intValue());
        st.setString(14, IssueHelper.getStringFromFieldArray(issue, "customfield_12601", ","));
        st.setString(15, IssueHelper.getValueFromFieldByKey(issue, "customfield_12606", "displayName"));
        st.setString(16, cg.getAuthor().getDisplayName());
        st.setTimestamp(17, new Timestamp(cg.getCreated().getMillis()));
        st.setString(18, IssueHelper.getStringFromFieldArray(issue, "customfield_12800", ","));
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }

}
