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

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;
import ru.ikss.jiratask.jira.IssueHelper;
import ru.ikss.jiratask.jira.JiraClient;

public class Set5Project extends Project {

    private static final Logger log = LoggerFactory.getLogger(Set5Project.class);
    private static final String INSERT_DATA = "select taskInsert(?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String GET_TIME = "select getLastTaskDate()";
    private static final String JQL = Config.getInstance().getValue("jira.jqlSet5");
    private static final String devTime = "customfield_12400";
    private static final String testTime = "customfield_12401";

    @Override
    public void handleTasks() {
        log.trace("Handle project {}", this.getClass().getSimpleName());
        DateTime lastTime = getLastTime();
        String jql = JQL.replace("%dbUpdateTime%", "\'" + lastTime.toString(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm")) + "\'");
        log.trace("JIRA query: " + jql);
        try (JiraRestClient client = JiraClient.getInstance().getClient();
                Connection con = DAO.I.getConnection();
                CallableStatement st = con.prepareCall(INSERT_DATA)) {
            for (String key : getAllTasks(client, jql)) {
                Issue issue = client.getIssueClient().getIssue(key, Collections.singletonList(Expandos.CHANGELOG)).claim();
                log.trace(issue.getKey() + "\t" + issue.getStatus().getName());
                for (ChangelogGroup cg : issue.getChangelog()) {
                    if (cg.getCreated().compareTo(lastTime) > 0) {
                        for (ChangelogItem ci : cg.getItems()) {
                            if (ci.getField().equals("status")) {
                                insertStatus(st, issue, cg, ci);
                            }
                        }
                    }
                }
            }
        } catch (SQLException | IOException e) {
            log.error("Error on handling project", e);
        }
        log.trace("End\n");
    }

    private static void insertStatus(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setInt(6, IssueHelper.getDoubleFromField(issue, devTime).intValue());
        st.setInt(7, IssueHelper.getDoubleFromField(issue, testTime).intValue());
        st.setString(8, IssueHelper.getFixVersions(issue));
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }
}
