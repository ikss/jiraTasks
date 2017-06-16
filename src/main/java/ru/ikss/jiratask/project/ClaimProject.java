package ru.ikss.jiratask.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;

public class ClaimProject extends Project {

    private static final Logger log = LoggerFactory.getLogger(ClaimProject.class);
    private static final String GET_TIME = "select ProblemsGetLastActionInfoDate()";
    private static final String INSERT_DATA = "select InsertProblems(?)";

    @Override
    public void handleTasks() {
        log.trace("Handle project {}", this.toString());
        try {
            DateTime lastTime = getLastTime();
            DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
            String from = lastTime.toString(datePattern);
            String to = new DateTime().toString(datePattern);

            String login = URLEncoder.encode(Config.getInstance().getValue("claim.login", "user"), "UTF-8");
            String pwd = URLEncoder.encode(Config.getInstance().getValue("claim.pwd", ""), "UTF-8");

            List<String> cookies = new ArrayList<>();

            String httpUrl = Config.getInstance().getValue("claim.url", "http://crm-beta.crystals.ru/WS/ClaimServiceAU.asmx");

            String logonUrl = httpUrl + "/Logon?login=" + login + "&pass=" + pwd;
            String answer = open(new URL(logonUrl), cookies);
            log.trace(answer);

            String getProblemsUrl =
                    httpUrl + "/GetProblems" + "?" + "fm=" + URLEncoder.encode(from, "UTF-8") + "&to=" + URLEncoder.encode(to, "UTF-8") +
                        "&deptID=" + Config.getInstance().getValue("claim.deptID", "-1");
            String xml = open(new URL(getProblemsUrl), cookies);
            if (xml != null) {
                log.trace("xml size = " + xml.length());
                try (Connection con = DAO.I.getConnection();
                        CallableStatement st = con.prepareCall(INSERT_DATA)) {
                    st.setString(1, xml);
                    st.execute();
                }
            } else {
                log.warn(this + "No data");
            }
        } catch (Exception e) {
            log.error(this + "Error on handling project", e);
        }
    }

    private String open(URL url, List<String> cookies) throws IOException, ProtocolException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.addRequestProperty("Cookie", cookies.stream().map(c -> c.split("~", 2)[0]).collect(Collectors.joining(";")));

        try (InputStream content = connection.getInputStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(content, "utf-8"))) {
            StringBuilder xml = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                xml.append(line);
            }
            cookies.clear();
            List<String> c = connection.getHeaderFields().get("Set-Cookie");
            if (c != null) {
                cookies.addAll(c);
            }
            return xml.toString();
        } catch (Exception e) {
            log.error(this + e.getMessage(), e);
            try (InputStream content = connection.getErrorStream()) {
                if (content != null) {
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(content, "utf-8"))) {
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

    @Override
    public String getTimeQuery() {
        return GET_TIME;
    }

}
