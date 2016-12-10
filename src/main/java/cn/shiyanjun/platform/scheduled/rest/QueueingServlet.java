package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.scheduled.common.QueueingManager;
import cn.shiyanjun.platform.scheduled.common.ResourceManagementProtocol;
import cn.shiyanjun.platform.scheduled.common.RestManageable;

public class QueueingServlet extends AbstractServlet {
	
	private static final long serialVersionUID = 1L;
	
	public QueueingServlet(ResourceManagementProtocol protocol) {
		super(protocol);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject res = new JSONObject();
		String type = request.getParameter("type");
		if(!Strings.isNullOrEmpty(type)) {
			String queue = request.getParameter("queue");
			String jobId = request.getParameter("jobId");
			final RestManageable restManageable = super.getRestManageable();
			final QueueingManager queueingManager = super.getQueueingManager();
			Set<String> queueingQueueNames = queueingManager.queueNames();
			try {
				switch(type) {
					case "prioritize":
						// http://127.0.0.1:8080/schedp/queueing?type=prioritize&queue=q_user_job&jobId=792
						// {"statusCode":200,"message":"SUCCESS"}
						checkQueue(queue, queueingQueueNames);
						checkJobId(jobId);
						prioritize(res, queue, jobId, restManageable);
						break;
						
					case "waitingJobs":
						// http://127.0.0.1:8080/schedp/queueing?type=waitingJobs&queue=q_user_job&jobId=792
						// {"statusCode":200,"message":"SUCCESS","jobs":[{"id":1274,"taskCount":5,"currentTask":3},{"id":1278,"taskCount":4,"currentTask":2}]}
						checkQueue(queue, queueingQueueNames);
						checkJobId(jobId);
						getWaitingJobs(res, queue, jobId, restManageable);
						break;	
						
					case "queueStatuses":
						// http://127.0.0.1:8080/schedp/queueing?type=queueStatuses
						// {"statuses":[{"q_enterprise_job":0},{"q_user_job":0}],"message":"SUCCESS","statusCode":200}
						queueStatuses(res, restManageable);
						break;
						
					case "jobStatuses":
						// http://127.0.0.1:8080/schedp/queueing?type=jobStatuses&queue=q_user_job
						// {"message":"SUCCESS","statusCode":200,"statuses":[{"taskCount":3,"currentTask":-1,"id":1024},{"taskCount":5,"currentTask":-1,"id":1}]}
						checkQueue(queue, queueingQueueNames);
						jobStatuses(res, queue, restManageable);
						break;
					default:
				}
			} catch (Exception e) {
				addStatusCode(res, 500);
				res.put(MESSAGE, e.getMessage());
			}
		} else {
			addStatusCode(res, 500);
			res.put(MESSAGE, "Bad parameter: type = null");
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

	private void jobStatuses(JSONObject res, String queue, final RestManageable restManageable) {
		JSONArray jobStatuses = new JSONArray();
		Map<Integer, JSONObject> jobs = restManageable.getQueuedJobStatuses(queue);
		for(int id : jobs.keySet()) {
			jobStatuses.add(jobs.get(id));
		}
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		res.put("statuses", jobStatuses);
	}

	private void queueStatuses(JSONObject res, final RestManageable restManageable) {
		Map<String, JSONObject> statuses = restManageable.getQueueStatuses();
		JSONArray queueStatuses = new JSONArray();
		for(String key : statuses.keySet()) {
			queueStatuses.add(statuses.get(key));
		}
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		res.put("statuses", queueStatuses);
	}

	private void getWaitingJobs(JSONObject res, String queue, String jobId, final RestManageable restManageable) {
		Collection<String> jobs = restManageable.getWaitingJobs(queue, jobId);
		JSONArray ja = new JSONArray();
		for(String job : jobs) {
			ja.add(JSONObject.parse(job));
		}
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
		res.put(JOBS, ja);
	}

	private void prioritize(JSONObject res, String queue, String jobId, final RestManageable restManageable) {
		int id = Integer.parseInt(jobId);
		restManageable.prioritize(queue, id);
		addStatusCode(res, 200);
		addMessage(res, "SUCCESS");
	}

	private void addMessage(JSONObject res, String message) {
		res.put(MESSAGE, message);
	}

	private void addStatusCode(JSONObject res, int code) {
		res.put(STATUS_CODE, code);
	}

}
