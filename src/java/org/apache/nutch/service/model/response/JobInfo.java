package org.apache.nutch.service.model.response;

import java.util.Map;

import org.apache.nutch.service.JobManager.JobType;

/**
 * This is the response object containing Job information
 * @author Sujen Shah
 *
 */
public class JobInfo {

	public static enum State {
		IDLE, RUNNING, FINISHED, FAILED, KILLED, STOPPING, KILLING, ANY
	};
	
	private String id;
	private JobType type;
	private String confId;
	private Map<String, Object> args;
	private Map<String, Object> result;
	private State state;
	private String msg;
	private String crawlId;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public JobType getType() {
		return type;
	}
	public void setType(JobType type) {
		this.type = type;
	}
	public String getConfId() {
		return confId;
	}
	public void setConfId(String confId) {
		this.confId = confId;
	}
	public Map<String, Object> getArgs() {
		return args;
	}
	public void setArgs(Map<String, Object> args) {
		this.args = args;
	}
	public Map<String, Object> getResult() {
		return result;
	}
	public void setResult(Map<String, Object> result) {
		this.result = result;
	}
	public State getState() {
		return state;
	}
	public void setState(State state) {
		this.state = state;
	}
	public String getMsg() {
		return msg;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public String getCrawlId() {
		return crawlId;
	}
	public void setCrawlId(String crawlId) {
		this.crawlId = crawlId;
	}
	
	


}
