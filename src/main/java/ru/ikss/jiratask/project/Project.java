package ru.ikss.jiratask.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.Issue;
import com.atlassian.jira.rest.client.api.domain.SearchResult;

import ru.ikss.jiratask.DAO;

public abstract class Project {

    private static final Logger log = LoggerFactory.getLogger(Project.class);
    private static final int MAX_RESULTS = 500;
    private static final Set<String> fields = Stream.of("summary", "issuetype", "created", "updated", "project", "status", "key")
        .collect(Collectors.toSet());

    public abstract void handleTasks();

    public abstract String getTimeQuery();

    public DateTime getLastTime() {
        return DAO.I.getTime(getTimeQuery());
    }

    public List<String> getAllTasks(JiraRestClient client, String query) {
        List<String> result = new ArrayList<>();
        int startAt = 0;
        boolean hasData = false;
        do {
            hasData = false;
            SearchResult searchResult = client.getSearchClient().searchJql(query, MAX_RESULTS, startAt, fields).claim();
            for (Issue issue : searchResult.getIssues()) {
                hasData = true;
                result.add(issue.getKey());
                startAt++;
            }
        } while (hasData);
        log.trace("Total works: {}", result.size());
        return result;
    }

}
