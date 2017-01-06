package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.scheduled.api.ComponentManager;

public class CancelJobServlet extends AbstractServlet {

	private static final Log LOG = LogFactory.getLog(CancelJobServlet.class);
	private static final long serialVersionUID = 1L;
	
	public CancelJobServlet(ComponentManager manager) {
		super(manager);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

	// http://127.0.0.1:8030/admin/cancelJob?jobId=1426
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject res = new JSONObject(true);
		String id = request.getParameter("jobId");
		
		JSONObject req = new JSONObject(true);
		addKV(req, "jobId", id);
    	LOG.info("Requested params: " + req.toJSONString());
		
		if(!Strings.isNullOrEmpty(id)) {
			try {
				int jobId = Integer.parseInt(id);
				boolean cancelled = super.getRestExporter().cancelJob(jobId);
				JSONObject val = new JSONObject(true);
				addKV(val, "cancelled", cancelled);
				
				addStatusCode(res, 200);
				addMessage(res, "OK");
				
				JSONObject a = new JSONObject();
				
				addKV(a, "current", val);
				
				addKV( res, "state", a);
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
	
}
