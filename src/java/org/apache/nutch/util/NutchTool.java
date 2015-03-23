package org.apache.nutch.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.metadata.Nutch;

public abstract class NutchTool extends Configured {

	protected HashMap<String, Object> results = new HashMap<String, Object>();
	protected Map<String, Object> status = Collections
			.synchronizedMap(new HashMap<String, Object>());
	protected Job currentJob;
	protected int numJobs;
	protected int currentJobNum;

	/**
	 * Runs the tool, using a map of arguments. May return results, or null.
	 */
	public abstract int run(Map<String, String> args)
			throws Exception;

	/** Returns relative progress of the tool, a float in range [0,1]. */
	public float getProgress() {
		float res = 0;
		if (currentJob != null) {
			try {
				res = (currentJob.mapProgress() + currentJob.reduceProgress()) / 2.0f;
			} catch (IOException e) {
				e.printStackTrace();
				res = 0;
			} catch (IllegalStateException ile) {
				ile.printStackTrace();
				res = 0;
			}
		}
		// take into account multiple jobs
		if (numJobs > 1) {
			res = (currentJobNum + res) / (float) numJobs;
		}
		status.put(Nutch.STAT_PROGRESS, res);
		return res;
	}

	/** Returns current status of the running tool. */
	public Map<String, Object> getStatus() {
		return status;
	}

	/**
	 * Stop the job with the possibility to resume. Subclasses should override
	 * this, since by default it calls {@link #killJob()}.
	 * 
	 * @return true if succeeded, false otherwise
	 */
	public boolean stopJob() throws Exception {
		return killJob();
	}

	/**
	 * Kill the job immediately. Clients should assume that any results that the
	 * job produced so far are in inconsistent state or missing.
	 * 
	 * @return true if succeeded, false otherwise.
	 * @throws Exception
	 */
	public boolean killJob() throws Exception {
		if (currentJob != null && !currentJob.isComplete()) {
			try {
				currentJob.killJob();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return false;
	}
}