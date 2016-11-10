package ru.ikss.jiratask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.ikss.jiratask.project.COPProject;
import ru.ikss.jiratask.project.CRProject;
import ru.ikss.jiratask.project.Project;
import ru.ikss.jiratask.project.Set10Project;
import ru.ikss.jiratask.project.Set5Project;

public class JiraTask {

    private static final Logger log = LoggerFactory.getLogger(JiraTask.class);

    private static List<Project> projects = new ArrayList<>();

    public static void main(String[] args) {
        Integer delay = Integer.valueOf(Config.getInstance().getValue("delay", "60"));
        log.trace("------ Work started ------");
        log.debug("Execute with delay = {}", delay);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        projects.add(new Set5Project());
        projects.add(new Set10Project());
        projects.add(new CRProject());
        projects.add(new COPProject());
        executor.scheduleWithFixedDelay(JiraTask::handleProjects, 0, delay, TimeUnit.MINUTES);
    }

    private static void handleProjects() {
        log.trace("------ Start handling projects ------");
        projects.forEach(p -> p.handleTasks());
        DAO.I.recalcData();
        log.trace("------ All projects handled ------");
    }

}
