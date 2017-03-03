package cn.shiyanjun.platform.scheduled.common;

public class RESTRequest {

	private final String jsonParams;
	private final int jobId;
	private final int jobType;
	
	public RESTRequest(String jsonParams, int jobId, int jobType) {
		super();
		this.jsonParams = jsonParams;
		this.jobId = jobId;
		this.jobType = jobType;
	}

	public String getJsonParams() {
		return jsonParams;
	}

	public int getJobId() {
		return jobId;
	}

	public int getJobType() {
		return jobType;
	}
}
