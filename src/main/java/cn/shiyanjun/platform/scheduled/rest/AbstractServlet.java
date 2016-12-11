package cn.shiyanjun.platform.scheduled.rest;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import cn.shiyanjun.platform.scheduled.common.QueueingManager;
import cn.shiyanjun.platform.scheduled.common.GlobalResourceManager;
import cn.shiyanjun.platform.scheduled.common.ResourceManager;
import cn.shiyanjun.platform.scheduled.common.RestManageable;
import cn.shiyanjun.platform.scheduled.common.SchedulingStrategy;

public abstract class AbstractServlet extends HttpServlet {
	
	protected static final String STATUS_CODE = "statusCode";
	protected static final String MESSAGE = "message";
	protected static final String JOBS = "jobs";
	
	private static final long serialVersionUID = 1L;
    private final RestManageable restManageable;
    private final QueueingManager queueingManager;
	private final SchedulingStrategy schedulingStrategy;
	private final ResourceManager resourceMetadataManager;
	protected final GlobalResourceManager protocol;

    public AbstractServlet(GlobalResourceManager protocol) {
        super();
        this.protocol = protocol;
        restManageable= protocol.getRestManageable();
        queueingManager = protocol.getQueueingManager();
        schedulingStrategy = protocol.getSchedulingStrategy();
        resourceMetadataManager = protocol.getResourceMetadataManager();
    }
    
    @Override
    public void init(ServletConfig config) throws ServletException {
    	super.init(config);
    }

	public RestManageable getRestManageable() {
		return restManageable;
	}

	public QueueingManager getQueueingManager() {
		return queueingManager;
	}

	public SchedulingStrategy getSchedulingStrategy() {
		return schedulingStrategy;
	}
	
	public ResourceManager getResourceMetadataManager() {
		return resourceMetadataManager;
	}
}
