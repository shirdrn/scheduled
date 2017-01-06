package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;

public class ResourceServlet extends AbstractServlet {

	private static final Log LOG = LogFactory.getLog(ResourceServlet.class);
	private static final long serialVersionUID = 1L;

	public ResourceServlet(ComponentManager manager) {
		super(manager);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

	// http://127.0.0.1:8030/admin/resource?queue=q_user_job&taskType=1&amount=2
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	String queue = request.getParameter("queue");
    	String taskType = request.getParameter("taskType");
    	String amount = request.getParameter("amount");
    	
    	JSONObject req = new JSONObject(true);
    	if(queue != null) {
    		req.put("queue", queue);
    	}
    	if(taskType != null) {
    		req.put("taskType", taskType);
    	}
    	if(amount != null) {
    		req.put("amount", amount);
    	}
    	LOG.info("Requested params: " + req.toJSONString());
    	
    	JSONObject result = new JSONObject();
    	result.put(JSONKeys.STATUS, 400);
    	result.put("message", "UNKNOWN");
    	if(!Strings.isNullOrEmpty(queue) 
    			&& !Strings.isNullOrEmpty(taskType) 
    			&& !Strings.isNullOrEmpty(amount)) {
    		try {
				TaskType type = TaskType.fromCode(Integer.parseInt(taskType)).get();
				int intAmount = Integer.parseInt(amount);
				super.getRestExporter().updateResourceAmount(queue, type, intAmount);
				addStatusCode(result, 200);
				result.put("message", "SUCCEEDED");
			} catch (Exception e) {
				result.put("message", e.toString());
			}
    	}
        response.getWriter().print(result.toJSONString());
    }
}
