package ru.ikss.jiratask.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class ClaimProject {

    private static final Logger log = LoggerFactory.getLogger(ClaimProject.class);
    private static final String GET_TIME_PROBLEMS = "select ProblemsGetLastActionInfoDate()";
    private static final String GET_TIME_PER_EQUIP = "select date_begin, date_end from ClaimPerEquipGetDates()";
    private static final String GET_TIME_BY_FILTERS = "select date_begin, date_end from ClaimGetDates()";
    private static final String INSERT_PROBLEMS = "select InsertProblems(?)";
    private static final String INSERT_PER_EQUIP = "select InsertClaimPerEquip(?)";
    private static final String INSERT_BY_FILTERS = "select InsertClaims(?)";

    private static final String UTF_8 = "UTF-8";
    private String logonUrl;
    private String httpUrl;

    public ClaimProject() {
        try {
            String login = URLEncoder.encode(Config.getInstance().getValue("claim.login", "user"), UTF_8);
            String pwd = URLEncoder.encode(Config.getInstance().getValue("claim.pwd", ""), UTF_8);
            httpUrl = Config.getInstance().getValue("claim.url", "http://crm-beta.crystals.ru/WS/ClaimServiceAU.asmx");
            logonUrl = httpUrl + "/Logon?login=" + login + "&pass=" + pwd;
        } catch (UnsupportedEncodingException e) {
            log.error(this + "Error on define login/pwd: " + e.getMessage(), e);
        }

    }

    public void getProblems() {
        log.trace("Handle Claim Problems");
        try {
            DateTime lastTime = DAO.I.getTime(GET_TIME_PROBLEMS);
            DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
            String from = lastTime.toString(datePattern);
            String to = new DateTime().toString(datePattern);

            List<String> cookies = new ArrayList<>();
            String answer = open(new URL(logonUrl), cookies);
            log.trace(answer);

            String url =
                    httpUrl + "/GetProblems" + "?" +
                        "fm=" + URLEncoder.encode(from, UTF_8) +
                        "&to=" + URLEncoder.encode(to, UTF_8) +
                        "&deptID=" + Config.getInstance().getValue("claim.deptID", "-1") +
                        "&emptyLongFields=" + "1".equals(Config.getInstance().getValue("claim.emptyLongFields", "1"));
            retrieveData(cookies, url, INSERT_PROBLEMS);
        } catch (Exception e) {
            log.error(this + "Error on handling Claim Problems: " + e.getMessage(), e);
        }
    }

    public void getClaimPerEquip() {
        log.trace("Handle Claim Per Equip");
        try {
            DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
            String from = null;
            String to = null;
            try (Statement st = DAO.I.getConnection().createStatement(); ResultSet rs = st.executeQuery(GET_TIME_PER_EQUIP)) {
                if (rs.next()) {
                    from = new DateTime(rs.getTimestamp(1)).toString(datePattern);
                    to = new DateTime(rs.getTimestamp(2)).toString(datePattern);
                }
            }
            List<String> cookies = new ArrayList<>();
            String answer = open(new URL(logonUrl), cookies);
            log.trace(answer);

            String url =
                    httpUrl + "/GetClaimPerEquipState" + "?" +
                        "fm=" + URLEncoder.encode(from, UTF_8) +
                        "&to=" + URLEncoder.encode(to, UTF_8) +
                        "&ids=" + Config.getInstance().getValue("claim.ids", "-1");
            retrieveData(cookies, url, INSERT_PER_EQUIP);
        } catch (Exception e) {
            log.error(this + "Error on handling Claim Per Equip: " + e.getMessage(), e);
        }
    }

    public void getClaimsByFilters() {
        log.trace("Handle Claims By Filters");
        try {
            DateTimeFormatter datePattern = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
            String from = null;
            String to = null;
            try (Statement st = DAO.I.getConnection().createStatement(); ResultSet rs = st.executeQuery(GET_TIME_BY_FILTERS)) {
                if (rs.next()) {
                    from = new DateTime(rs.getTimestamp(1)).toString(datePattern);
                    to = new DateTime(rs.getTimestamp(2)).toString(datePattern);
                }
            }
            List<String> cookies = new ArrayList<>();
            String answer = open(new URL(logonUrl), cookies);
            log.trace(answer);

            String url =
                    httpUrl + "/GetClaimsByFilters" + "?" +
                        "fm=" + URLEncoder.encode(from, UTF_8) +
                        "&to=" + URLEncoder.encode(to, UTF_8) +
                        "&compID=" + Config.getInstance().getValue("claim.compID", "-1");
            retrieveData(cookies, url, INSERT_BY_FILTERS);
        } catch (Exception e) {
            log.error(this + "Error on handling Claims By Filters: " + e.getMessage(), e);
        }
    }

    private void retrieveData(List<String> cookies, String url, String insertData)
        throws IOException, SQLException {
        String xml = open(new URL(url), cookies);
        if (xml != null) {
            log.trace("xml size = {}", xml.length());
            try (Connection con = DAO.I.getConnection();
                    CallableStatement st = con.prepareCall(insertData)) {
                st.setString(1, xml);
                st.execute();
            }
        } else {
            log.warn("{} No data", this);
        }
    }

    private String open(URL url, List<String> cookies) throws IOException {
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
}
