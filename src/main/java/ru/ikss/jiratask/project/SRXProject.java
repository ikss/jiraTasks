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

public class SRXProject extends Project {

    private static final Logger log = LoggerFactory.getLogger(SRXProject.class);
    private static final String INSERT_DATA = "select SRXTaskInsert(?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String GET_TIME = "select SRXGetLastTaskDate()";
    private static final String JQL = Config.getInstance().getValue("jira.jqlSRX");

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
                if (issue.getCreationDate().compareTo(lastTime) > 0) {
                    createTask(st, issue);
                }
            }
        } catch (SQLException | IOException e) {
            log.error("Error on handling project", e);
        }
        log.trace("End\n");
    }

    private static void createTask(CallableStatement st, Issue issue) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(issue.getCreationDate().getMillis()));
        st.setString(4, null);
        st.setString(5, "Новая");
        st.setInt(6, IssueHelper.getIntFromField(issue, "aggregatetimespent"));
        st.setString(7, IssueHelper.getFixVersions(issue));
        st.setString(8, issue.getPriority().getName());
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    private static void insertStatus(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setInt(6, IssueHelper.getIntFromField(issue, "aggregatetimespent"));
        st.setString(7, IssueHelper.getFixVersions(issue));
        st.setString(8, issue.getPriority().getName());
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }

}
