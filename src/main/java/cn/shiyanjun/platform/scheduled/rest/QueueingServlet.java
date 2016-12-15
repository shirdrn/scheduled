package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.RestExporter;

public class QueueingServlet extends AbstractServlet {
	
	private static final Log LOG = LogFactory.getLog(QueueingServlet.class);
	private static final long serialVersionUID = 1L;
	
	public QueueingServlet(ComponentManager protocol) {
		super(protocol);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject res = new JSONObject();
		String type = request.getParameter("type");
		String queue = request.getParameter("queue");
		String jobId = request.getParameter("jobId");
		
		JSONObject req = new JSONObject(true);
		addKV(req, "type", type);
		addKV(req, "queue", queue);
		addKV(req, "jobId", jobId);
    	LOG.info("Requested params: " + req.toJSONString());
    	
		if(!Strings.isNullOrEmpty(type)) {
			final RestExporter restExporter = super.getRestExporter();
			Set<String> queueingQueueNames = restExporter.queueingNames();
			try {
				switch(type) {
					case "prioritize":
						// http://127.0.0.1:8030/admin/queueing?type=prioritize&queue=q_user_job&jobId=792
						// {"statusCode":200,"message":"SUCCESS"}
						checkQueue(queue, queueingQueueNames);
						checkJobId(jobId);
						prioritize(res, queue, jobId, restExporter);
						break;
						
					case "waitingJobs":
						// http://127.0.0.1:8030/admin/queueing?type=waitingJobs&queue=q_user_job&jobId=792
						// {"statusCode":200,"message":"SUCCESS","jobs":[{"id":1274,"taskCount":5,"currentTask":3},{"id":1278,"taskCount":4,"currentTask":2}]}
						checkQueue(queue, queueingQueueNames);
						checkJobId(jobId);
						getWaitingJobs(res, queue, jobId, restExporter);
						break;	
						
					case "queueStatuses":
						// http://127.0.0.1:8030/admin/queueing?type=queueStatuses
						// {"statuses":[{"q_enterprise_job":0},{"q_user_job":0}],"message":"SUCCESS","statusCode":200}
						queueStatuses(res, restExporter);
						break;
						
					case "jobStatuses":
						// http://127.0.0.1:8030/admin/queueing?type=jobStatuses&queue=q_user_job
						// {"message":"SUCCESS","statusCode":200,"statuses":[{"taskCount":3,"currentTask":-1,"id":1024},{"taskCount":5,"currentTask":-1,"id":1}]}
						checkQueue(queue, queueingQueueNames);
						jobStatuses(res, queue, restExporter);
						break;
					default:
				}
			} catch (Exception e) {
				addStatusCode(res, 500);
				addMessage(res, e.getMessage());
			}
		} else {
			addStatusCode(res, 500);
			addMessage(res, "Bad parameter: type = null");
		}
		response.getWriter().print(res.toJSONString());
	}
	
	private void checkQueue(String queue, Set<String> queueingQueueNames) {
		if(Strings.isNullOrEmpty(queue)) {
			throw new IllegalArgumentException("Bad parameter: queue = null");
		}
		if(!queueingQueueNames.contains(queue)) {
			throw new IllegalArgumentException("Bad parameter: Unknown queue, name = " + queue);
		}
	}
	
	private void checkJobId(String jobId) {
		if(Strings.isNullOrEmpty(jobId)) {
			throw new IllegalArgumentException("Bad parameter: jobId = null");
		}
	}

	private void jobStatuses(JSONObject res, String queue, final RestExporter restManageable) {
		JSONArray jobStatuses = new JSONArray();
		Map<Integer, JSONObject> jobs = restManageable.getQueuedJobStatuses(queue);
		jobs.keySet().forEach(id -> jobStatuses.add(jobs.get(id)));
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		addKV(res, "statuses", jobStatuses);
	}

	private void queueStatuses(JSONObject res, final RestExporter restManageable) {
		Map<String, JSONObject> statuses = restManageable.getQueueStatuses();
		JSONArray queueStatuses = new JSONArray();
		statuses.keySet().forEach(key -> queueStatuses.add(statuses.get(key)));
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		addKV(res, "statuses", queueStatuses);
	}

	private void getWaitingJobs(JSONObject res, String queue, String jobId, final RestExporter restManageable) {
		Collection<String> jobs = restManageable.getWaitingJobs(queue, jobId);
		JSONArray ja = new JSONArray();
		jobs.forEach(job -> ja.add(JSONObject.parse(job)));
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		addKV(res, "jobs", ja);
	}

	private void prioritize(JSONObject res, String queue, String jobId, final RestExporter restManageable) {
		int id = Integer.parseInt(jobId);
		restManageable.prioritize(queue, id);
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
	}

}
