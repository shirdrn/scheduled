package cn.shiyanjun.platform.scheduled.rest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;

import cn.shiyanjun.platform.api.utils.Pair;
import cn.shiyanjun.platform.scheduled.api.ComponentManager;

public class MaintenanceServlet extends AbstractServlet {

	private static final Log LOG = LogFactory.getLog(MaintenanceServlet.class);
	private static final long serialVersionUID = 1L;
	
	public MaintenanceServlet(ComponentManager protocol) {
		super(protocol);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

	// http://127.0.0.1:8030/admin/maintenance?startTime=03:00:00&endTime=04:00:00
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject res = new JSONObject(true);
		String startTime = request.getParameter("startTime");
		String endTime = request.getParameter("endTime");
		
		JSONObject req = new JSONObject(true);
		addKV(req, "startTime", startTime);
		addKV(req, "endTime", endTime);
    	LOG.info("Requested params: " + req.toJSONString());
		
		if(!Strings.isNullOrEmpty(startTime) && !Strings.isNullOrEmpty(endTime)) {
			try {
				Pair<String, String> current = super.getRestExporter().queryMaintenanceTimeSegment();
				if(current != null) {
					addStatusCode(res, 200);
					addMessage(res, "OK");
					
					JSONObject val = new JSONObject(true);
					addKV(val, "currentStartTime", current.getKey());
					addKV(val, "currentEndTime", current.getValue());
					
					super.getRestExporter().updateMaintenanceTimeSegment(startTime, endTime);
					
					addKV(val, "updatedStartTime", startTime);
					addKV(val, "updatedEndtTime", endTime);
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
	
}
