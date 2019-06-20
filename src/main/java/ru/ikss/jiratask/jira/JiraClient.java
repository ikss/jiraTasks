package ru.ikss.jiratask.jira;

import java.net.URI;

import com.atlassian.jira.rest.client.api.JiraRestClient;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;

import ru.ikss.jiratask.Config;

public class JiraClient {

    private static final AsynchronousJiraRestClientFactory factory = new AsynchronousJiraRestClientFactory();
    private static final URI SERVER_URI = URI.create("https://crystals.atlassian.net");
    private static final String LOGIN = Config.getInstance().getValue("jira.login");
    private static final String PASSWORD = Config.getInstance().getValue("jira.pwd");
    private static JiraClient instance;

    public synchronized static JiraClient getInstance() {
        if (instance == null) {
            instance = new JiraClient();
        }
        return instance;
    }

    public JiraRestClient getClient() {
        return factory.createWithBasicHttpAuthentication(SERVER_URI, LOGIN, PASSWORD);
    }
}
