package cn.shiyanjun.platform.scheduled.rest;

import java.util.Objects;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import com.alibaba.fastjson.JSONObject;

import cn.shiyanjun.platform.scheduled.api.ComponentManager;
import cn.shiyanjun.platform.scheduled.api.RestExporter;

public abstract class AbstractServlet extends HttpServlet {
	
	private static final String STATUS_CODE = "statusCode";
	private static final String MESSAGE = "message";
	
	private static final long serialVersionUID = 1L;
    private final RestExporter restExporter;
	protected final ComponentManager manager;

    public AbstractServlet(ComponentManager manager) {
        super();
        this.manager = manager;
        restExporter= manager.getRestExporter();
    }
    
    @Override
    public void init(ServletConfig config) throws ServletException {
    	super.init(config);
    }

	protected RestExporter getRestExporter() {
		return restExporter;
	}
	
	protected <T> void addKV(JSONObject res, String key, T value) {
		if(Objects.isNull(value)) {
			res.put(key, "");
		} else {
			res.put(key, value);
		}
	}
	
	protected void addMessage(JSONObject res, String message) {
		res.put(MESSAGE, message);
	}
	
	protected void addStatusCode(JSONObject res, int code) {
		res.put(STATUS_CODE, code);
	}
}
