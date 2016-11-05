package ru.ikss.jiratask.project;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.api.domain.SearchResult;

import ru.ikss.jiratask.DAO;

public abstract class Project {

    private static final Logger log = LoggerFactory.getLogger(Project.class);

    public abstract void handleTasks();

    public DateTime getLastTime() {
        return DAO.I.getTime(getTimeQuery());
    }

    public abstract String getTimeQuery();

    public List<String> getAllTasks(JiraRestClient client, String query) {
        List<String> result = new ArrayList<>();
        SearchResult searchResult = client.getSearchClient().searchJql(query).claim();
        log.trace("Total works: {}", searchResult.getTotal());
        searchResult.getIssues().forEach(i -> result.add(i.getKey()));
        return result;
    }

}
