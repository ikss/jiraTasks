package ru.ikss.jiratask;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.xml.ws.Endpoint;
import javax.xml.ws.soap.SOAPBinding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;

import ru.ikss.jiratask.project.COPProject;
import ru.ikss.jiratask.project.CRProject;
import ru.ikss.jiratask.project.ClaimProject;
import ru.ikss.jiratask.project.LSNProject;
import ru.ikss.jiratask.project.MachineProject;
import ru.ikss.jiratask.project.ProjectTask;
import ru.ikss.jiratask.project.SRXProject;
import ru.ikss.jiratask.project.Set10Project;
import ru.ikss.jiratask.project.Set5Project;
import ru.ikss.jiratask.project.TOUCHProject;
import ru.ikss.jiratask.project.UXProject;
import ru.ikss.jiratask.project.WalletProject;
import ru.ikss.jiratask.project._1C;
import ru.ikss.jiratask.ws.VersionManager;

public class JiraTask {

    private static final Logger log = LoggerFactory.getLogger(JiraTask.class);

    private static List<ProjectTask> projects = new ArrayList<>();

    public static void main(String[] args) {
        Integer delay = Integer.valueOf(Config.getInstance().getValue("delay", "60"));
        log.trace("------ Work started ------");
        log.debug("Execute with delay = {}", delay);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        if ("1".equals(Config.getInstance().getValue("test", "0"))) {
            projects.add(new Set10Project());
        } else {
            ClaimProject claimProject = new ClaimProject();
            projects.add(claimProject::getProblems);
            projects.add(claimProject::getClaimPerEquip);
            projects.add(new Set5Project());
            projects.add(new Set10Project());
            projects.add(new CRProject());
            projects.add(new COPProject());
            projects.add(new UXProject());
            projects.add(new SRXProject());
            projects.add(new TOUCHProject());
            projects.add(new LSNProject());
            projects.add(new MachineProject());
            projects.add(new WalletProject());
            start1C();
        }
        executor.scheduleWithFixedDelay(JiraTask::handleProjects, 0, delay, TimeUnit.MINUTES);

        Integer sp = Integer.parseInt(Config.getInstance().getValue("WSPort", "0"));
        if (sp > 0) {
            InetSocketAddress addr = new InetSocketAddress(sp);
            HttpServer server;
            try {
                server = HttpServer.create(addr, 0);
                HttpContext soapContext = server.createContext("/VersionManager");
                server.setExecutor(Executors.newCachedThreadPool());
                server.start();
                Endpoint endpoint = Endpoint.create(SOAPBinding.SOAP11HTTP_BINDING, new VersionManager());
                endpoint.publish(soapContext);

                log.debug("Веб-сервер http://0.0.0.0:" + sp + "/VersionManager запущен!");
            } catch (IOException e) {
                log.error("Ошибка создания веб-сервиса: " + e.getMessage(), e);
            }
        }
    }

    private static void start1C() {
        ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);
        Integer delay = Integer.valueOf(Config.getInstance().getValue("1C.delay", "60"));
        pool.scheduleWithFixedDelay(_1C::process, 0, delay, TimeUnit.MINUTES);
    }

    private static void handleProjects() {
        log.trace("------ Start handling projects ------");
        if (!Config.getInstance().getValue("sequential", "0").equals("1")) {
            ExecutorService executor = Executors.newFixedThreadPool(projects.size());
            try {
                executor.invokeAll(projects.stream().map(project -> (Callable<Void>) (() -> {
                    try {
                        project.handleTasks();
                    } catch (Throwable e) {
                        log.error(project.getClass().getSimpleName() + " error: " + e.getMessage(), e);
                    }
                    return null;
                })).collect(Collectors.toList()));
            } catch (InterruptedException e1) {
                log.error("InterruptedException: " + e1.getMessage(), e1);
            }
        } else {
            for (ProjectTask project : projects) {
                try {
                    project.handleTasks();
                } catch (Throwable e) {
                    log.error(project.getClass().getSimpleName() + " error: " + e.getMessage(), e);
                }
            }
        }
        DAO.I.recalcData();
        log.trace("------ All projects handled ------");
    }
}
