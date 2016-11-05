package ru.ikss.jiratask.project;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
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
import com.atlassian.jira.rest.client.api.domain.User;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;
import ru.ikss.jiratask.jira.IssueHelper;
import ru.ikss.jiratask.jira.JiraClient;

public class Set10Project extends Project {

    private static final Logger log = LoggerFactory.getLogger(Set10Project.class);
    private static final String INSERT_DATA = "select set10TaskInsert(?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
                CallableStatement st = con.prepareCall(INSERT_DATA)) {
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
                for (ChangelogGroup cg : issue.getChangelog()) {
                    if (cg.getCreated().compareTo(lastTime) > 0) {
                        for (ChangelogItem ci : cg.getItems()) {
                            if (ci.getField().equals("status")) {
                                insertStatus(st, issue, cg, ci, team);
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

    private static void insertStatus(CallableStatement st, Issue issue, ChangelogGroup cg, ChangelogItem ci, String team) throws SQLException {
        st.setString(1, issue.getKey());
        st.setString(2, issue.getIssueType().getName());
        st.setTimestamp(3, new Timestamp(cg.getCreated().getMillis()));
        st.setString(4, ci.getFromString());
        st.setString(5, ci.getToString());
        st.setString(6, team);
        st.setInt(7, IssueHelper.getIntFromField(issue, "aggregatetimeoriginalestimate"));
        st.setInt(8, IssueHelper.getIntFromField(issue, "aggregatetimespent"));
        st.setString(9, IssueHelper.getFixVersions(issue));
        log.debug("query: '{}'", st.toString());
        st.execute();
    }

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }

}