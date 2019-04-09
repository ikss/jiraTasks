package ru.ikss.jiratask.project;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.Config;
import ru.ikss.jiratask.DAO;

public class _1C {

    private static final Logger log = LoggerFactory.getLogger(_1C.class);

    public static void process() {
        log.trace("Handle project 1C");
        try {
            String req = getPeriodFor1C();
            if (req != null && !req.isEmpty()) {
                log.trace("1C\t> " + req);
                getData(req, "getProductSales", "1C.url", "insertDataFrom1C");
                getData(req, "Vacation", "1C.Vacation.url", "insertVacation1C");
            }
        } catch (Throwable e) {
            log.error("1C\t " + e.getMessage(), e);
        }
    }

    private static void getData(String req, String toString, String urlKey, String procedure) {
        try {
            String ans = getDataFrom1C(urlKey, req);
            log.trace("1C." + toString + "\t< " + ans);
            insertDataFrom1C(ans, procedure);
        } catch (SQLException | IOException e) {
            log.error("1C." + toString + "\t " + e.getMessage(), e);
        }
    }

    private static String getPeriodFor1C() {
        String result = null;
        try (Statement st = DAO.I.getConnection().createStatement(); ResultSet rs = st.executeQuery("select getPeriodFor1C()")) {
            if (rs.next())
                result = rs.getString(1);
        } catch (SQLException e) {
            log.error("Error on getting date time with query {}", "getPeriodFor1C", e);
        }
        return result;
    }

    private static String getDataFrom1C(String urlKey, String bodyStr)
        throws UnsupportedEncodingException, MalformedURLException, IOException, ProtocolException {
        byte[] outputByte = bodyStr.getBytes("utf-8");

        String login = URLEncoder.encode(Config.getInstance().getValue("1C.login", "user"), "UTF-8");
        String pwd = URLEncoder.encode(Config.getInstance().getValue("1C.pwd", ""), "UTF-8");
        String httpUrl = Config.getInstance().getValue(urlKey, "http://1C-host/.../Synchronization/method");
        URL url = new URL(httpUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setUseCaches(false);
        conn.setRequestMethod("POST");
        String userpass = login + ":" + pwd;
        String basicAuth = "Basic " + Base64.getEncoder().encodeToString(userpass.getBytes());
        conn.addRequestProperty("Authorization", basicAuth);
        conn.addRequestProperty("Content-Type", "application/json"); // ; charset=utf-8
        conn.addRequestProperty("Content-Length", "" + outputByte.length);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(outputByte, 0, outputByte.length);
            os.flush();
        }

        try {
            String ans = readStream(conn, conn.getInputStream());
            return ans;
        } catch (Exception e) {
            // String err = readStream(conn, conn.getErrorStream());
            // log.error(err + e.getMessage(), e);
        }
        return null;
    }

    private static void insertDataFrom1C(String ans, String procedure) throws SQLException {
        if (ans != null && !ans.isEmpty()) {
            try (Connection con = DAO.I.getConnection();
                    CallableStatement st = con.prepareCall("select " + procedure + "(?)")) {
                st.setString(1, ans);
                st.execute();
            }
        }
    }

    private static String readStream(HttpURLConnection conn, InputStream inputStream) throws IOException {
        int size = Integer.parseInt(conn.getHeaderField("Content-Length"));
        if (size > 0) {
            byte[] buf = new byte[size];
            try (BufferedInputStream bis = new BufferedInputStream(inputStream, size)) {
                readAll(size, buf, bis);
            }
            return new String(buf, Charset.forName("UTF-8"));
        }
        return "";
    }

    private static void readAll(int size, byte[] buf, BufferedInputStream bis) throws IOException {
        int pos = 0;
        do {
            int count = bis.read(buf, pos, size - pos);
            if (count > 0) {
                pos += count;
            } else {
                throw new IOException("Too few data!!!");
            }
        } while (pos < size);
    }

    public static void main(String[] args) {
        process();
    }
}
