package cn.shiyanjun.platform.scheduled.protocols;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import cn.shiyanjun.platform.api.Context;
import cn.shiyanjun.platform.scheduled.api.Protocol;
import cn.shiyanjun.platform.scheduled.common.AbstractProtocolManager;
import cn.shiyanjun.platform.scheduled.common.RESTRequest;
import cn.shiyanjun.platform.scheduled.constants.ProtocolType;

public final class JobOrchestrationProtocolManager extends AbstractProtocolManager<Map<Integer, Protocol<RESTRequest, JSONObject>>> {

	private static final Log LOG = LogFactory.getLog(JobOrchestrationProtocolManager.class);
	private static final String ORCHESTRATION_ROUTER_PREFIX = "router.job.orchestrate";
	
	public JobOrchestrationProtocolManager(Context context) {
		super(context);
	}
	
	@Override
	public void initialize() {
		// parse router configuration items
		final Map<Integer, String> table = Maps.newHashMap();
		context.keyIterator().forEachRemaining(keyObj -> {
			try {
				String key = keyObj.toString();
				if(key.startsWith(ORCHESTRATION_ROUTER_PREFIX)) {
					String value = context.get(key);
					String[] kv = value.split(";");
					int jobType = Integer.parseInt(kv[0]);
					String url = kv[1];
					table.put(jobType, url);
				}
			} catch (Exception e) {
				LOG.warn("Bad router config: ", e);
			}
		});
		
		// register protocol objects
		Map<Integer, Protocol<RESTRequest, JSONObject>> protocols = Maps.newHashMap();
		table.keySet().forEach(jobType -> {
			String serviceUrl = table.get(jobType);
			protocols.put(jobType, new RESTJobOrchestrater(serviceUrl));
		});
		register(ProtocolType.REST, protocols);
	}

	private class RESTJobOrchestrater implements Protocol<RESTRequest, JSONObject> {

		private final String jobOrchestrateServiceUrl;
		private final OkHttpClient client = new OkHttpClient();
		private final MediaType mediaType = MediaType.parse("application/json");
		
		public RESTJobOrchestrater(String serviceUrl) {
			jobOrchestrateServiceUrl = serviceUrl;
			Preconditions.checkArgument(jobOrchestrateServiceUrl != null, "Job Orchestrate Service url shouldn't be null!");
			LOG.info("Configs: jobOrchestrateServiceUrl=" + jobOrchestrateServiceUrl);
		}
		
		@Override
		public JSONObject request(RESTRequest in) {
			JSONObject jobData = new JSONObject();
			// build a job from known JSON parameters
			int jobId = in.getJobId();
			int jobType = in.getJobType();
			String jsonParams = in.getJsonParams();
			Request request = buildHttpRequest(jsonParams, jobType, jobId);
			LOG.info("Prepare to request job orchestrate service: " + request);
			try {
				Response response = client.newCall(request).execute();
				if(response != null && response.isSuccessful()) {
					String result = response.body().string();
					LOG.info("Responsed orchestrated job: " + result);
					jobData = JSONObject.parseObject(result);
					// release all system resources
					response.body().close();
				} else {
					LOG.warn("Fail to parse & orchestrate job: jobId=" + jobId + ", jobType=" + jobType + ", params=" + jsonParams);
				}
			} catch (IOException e) {
				LOG.warn("Failed to orchestrate job: ", e);
			}
			return jobData;
		}
		
		private Request buildHttpRequest(String jobParameters, int jobType, int jobId) {
			String url = jobOrchestrateServiceUrl + "/" + jobType + "/" + jobId;
			RequestBody body = RequestBody.create(mediaType, jobParameters);
			Request request = new Request.Builder()
					.url(url)
					.post(body)
					.addHeader("content-type", "application/json")
					.addHeader("cache-control", "no-cache")
					.build();
			return request;
		}
	}
	
	@Override
	public Enum<?> ofType(String protocolType) {
		return ProtocolType.valueOf(protocolType);
	}
	
}
