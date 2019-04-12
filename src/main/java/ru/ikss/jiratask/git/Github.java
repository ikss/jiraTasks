package ru.ikss.jiratask.git;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

import org.eclipse.egit.github.core.PullRequest;
import org.eclipse.egit.github.core.RepositoryId;
import org.eclipse.egit.github.core.service.PullRequestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;

public class Github {

    private static final Logger log = LoggerFactory.getLogger(Github.class);

    private static final String LOGIN = Config.getInstance().getValue("github.login");
    private static final String PASSWORD = Config.getInstance().getValue("github.pwd");

    private static JsonMapper jm = JsonMapper.getInstance(false);

    public void getPullRequestInfo() {
        PullRequestNumbers numbers = getNumbers();

        if (numbers == null) {
            return;
        }

        if (numbers.getOpenNumbers() != null) {
            numbers.getOpenNumbers().stream()
                .map(this::getInfo)
                .filter(Objects::nonNull)
                .forEach(this::save);
        }

        if (numbers.getLastNumber() != null) {
            int i = numbers.getLastNumber() + 1;
            String info = getInfo(i);
            while (info != null) {
                save(info);
                i++;
                info = getInfo(i);
            }
        }
    }

    private static PullRequestNumbers getNumbers() {
        try {
            StringBuilder json = new StringBuilder();
            try (Statement st = DAO.I.getConnection().createStatement(); ResultSet rs = st.executeQuery("select get_pull_request()")) {
                while (rs.next()) {
                    json.append(rs.getString(1));
                }
            }
            return jm.readValue(json.toString(), PullRequestNumbers.class);
        } catch (IOException | SQLException e) {
            log.error("Error on get_pull_request", e);
            return null;
        }
    }

    private String getInfo(Integer prNumber) {
        PullRequestService service = new PullRequestService();
        service.getClient().setCredentials(LOGIN, PASSWORD);
        RepositoryId repo = new RepositoryId("crystalservice", "setretail10");
        try {
            PullRequest pullRequest = service.getPullRequest(repo, prNumber);
            if (pullRequest != null) {
                log.debug("github PR #{}\t{}\t{}", prNumber, pullRequest.getTitle(), pullRequest.getHead().getRef());
                return jm.writeValue(pullRequest);
            }
        } catch (IOException e) {
            log.error("Error on getInfo", e);
        }
        return null;
    }

    private void save(String info) {
        try (Connection con = DAO.I.getConnection();
                CallableStatement st = con.prepareCall("select insert_pull_request(?::JSON)")) {
            st.setString(1, info);
            st.execute();
        } catch (SQLException e) {
            log.error("Error on insert_pull_request", e);
        }
    }
}
