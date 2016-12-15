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

public class SchedulingServlet extends AbstractServlet {

	private static final Log LOG = LogFactory.getLog(SchedulingServlet.class);
	private static final long serialVersionUID = 1L;
	
	public SchedulingServlet(ComponentManager protocol) {
		super(protocol);
	}
	
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doPost(request, response);
    }

	// http://127.0.0.1:8030/admin/scheduling?isSchedulingOpened=true
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		JSONObject res = new JSONObject(true);
		String isSchedulingOpened = request.getParameter("isSchedulingOpened");
		
		JSONObject req = new JSONObject(true);
		addKV(req, "isSchedulingOpened", isSchedulingOpened);
    	LOG.info("Requested params: " + req.toJSONString());
		
		if(!Strings.isNullOrEmpty(isSchedulingOpened)) {
			try {
				boolean isOpened = Boolean.parseBoolean(isSchedulingOpened);
				boolean current = super.getRestExporter().isSchedulingOpened();
				if(isOpened) {
					if(current) {
						LOG.warn("Already opened!");
					} else {
						super.getRestExporter().setSchedulingOpened(isOpened);
					}
				} else {
					if(!current) {
						LOG.warn("Already closed!");
					} else {
						super.getRestExporter().setSchedulingOpened(isOpened);
					}
				}
				addStatusCode(res, 200);
				addMessage(res, "OK");
				
				JSONObject a = new JSONObject();
				
				JSONObject val = new JSONObject(true);
				addKV(val, "current", current);
				addKV(val, "request", isOpened);
				addKV(val, "updated", isOpened);
				
				addKV(a, "isSchedulingOpened", val);
				
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
