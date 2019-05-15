package ru.ikss.jiratask.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import javax.net.ssl.HttpsURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;

public class HdProject {

    private static final Logger log = LoggerFactory.getLogger(HdProject.class);
    private static final String INSERT_CLIENTS_VERSION = "select ClientVersionInsert(?::JSON)";

    private static final String UTF_8 = "UTF-8";
    private String clientsVersionUrl;

    public HdProject() {
        this.clientsVersionUrl = Config.getInstance().getValue("hd.ClientsVersion.url",
            "https://hd-beta.crystals.ru/api/ClientsVersion/Get?SecretKey=6GPPHx378e9EF256ZGilKBp964");

    }

    public void getClientsVersion() {
        log.trace("HD: get clients version");
        try {
            retrieveData(clientsVersionUrl, INSERT_CLIENTS_VERSION);
        } catch (Exception e) {
            log.error(this + "Error on handling Claim Problems: " + e.getMessage(), e);
        }
    }

    private void retrieveData(String url, String insertData)
        throws IOException, SQLException {
        String json = open(new URL(url));
        if (json != null) {
            log.trace("json size = {}", json.length());
            try (Connection con = DAO.I.getConnection();
                    CallableStatement st = con.prepareCall(insertData)) {
                st.setString(1, json);
                st.execute();
            }
        } else {
            log.warn("{} No data", this);
        }
    }

    private String open(URL url) throws IOException {
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        try (InputStream content = connection.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(content, UTF_8))) {
            StringBuilder ans = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                ans.append(line);
            }
            return ans.toString();
        } catch (Exception e) {
            log.error(this + e.getMessage(), e);
            try (InputStream content = connection.getErrorStream()) {
                if (content != null) {
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(content, UTF_8))) {
                        String line;
                        while ((line = in.readLine()) != null) {
                            log.error(line);
                        }
                    }
                }
            }
            return null;
        }
    }
}
