package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.api.constants.JSONKeys;
import cn.shiyanjun.platform.api.constants.TaskType;
import cn.shiyanjun.platform.scheduled.common.ResourceManagementProtocol;
import cn.shiyanjun.platform.scheduled.common.ResourceMetadataManager;

public class ResourceServlet extends AbstractServlet {

	private static final long serialVersionUID = 1L;

	public ResourceServlet(ResourceManagementProtocol protocol) {
		super(protocol);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	String queue = request.getParameter("queue");
    	String taskType = request.getParameter("taskType");
    	String amount = request.getParameter("amount");
    	JSONObject result = new JSONObject();
    	result.put(JSONKeys.STATUS, 400);
    	result.put("message", "UNKNOWN");
    	if(!Strings.isNullOrEmpty(queue) 
    			&& !Strings.isNullOrEmpty(taskType) 
    			&& !Strings.isNullOrEmpty(amount)) {
    		try {
				TaskType type = TaskType.fromCode(Integer.parseInt(taskType)).get();
				int intAmount = Integer.parseInt(amount);
				final ResourceMetadataManager resourceMetadataManager = this.getResourceMetadataManager();
				resourceMetadataManager.updateResourceAmount(queue, type, intAmount);
				result.put(JSONKeys.STATUS, 200);
				result.put("message", "SUCCEEDED");
			} catch (Exception e) {
				result.put("message", e.toString());
			}
    	}
        response.getWriter().print(result.toJSONString());
    }
}
