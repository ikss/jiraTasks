package ru.ikss.jiratask.ws;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.DAO;

@WebService
public class VersionManager {

    private static final Logger log = LoggerFactory.getLogger(VersionManager.class);
    private static final String VERSION_STAGE_START = "select versionStageStart(?, ?, ?, ?);";
    private static final String VERSION_STAGE_FINISH = "select versionStageFinish(?, ?, ?);";

    @WebMethod
    public void versionStageStart(@WebParam String version, @WebParam String stage, @WebParam Date dateBegin, @WebParam Date dateEndEstimate) {
        try (Connection con = DAO.I.getConnection();
                CallableStatement st = con.prepareCall(VERSION_STAGE_START);) {
            st.setString(1, version);
            st.setString(2, stage);
            st.setTimestamp(3, new Timestamp(dateBegin.getTime()));
            st.setTimestamp(4, new Timestamp(dateEndEstimate.getTime()));
            log.debug("query: '{}'", st.toString());
            st.execute();
        } catch (SQLException e) {
            log.error("Error on versionStageStart", e);
        }
    }

    @WebMethod
    public void versionStageFinish(@WebParam String version, @WebParam String stage, @WebParam Date dateEnd) {
        try (Connection con = DAO.I.getConnection();
                CallableStatement st = con.prepareCall(VERSION_STAGE_FINISH);) {
            st.setString(1, version);
            st.setString(2, stage);
            st.setTimestamp(3, new Timestamp(dateEnd.getTime()));
            log.debug("query: '{}'", st.toString());
            st.execute();
        } catch (SQLException e) {
            log.error("Error on versionStageFinish", e);
        }
    }
}
