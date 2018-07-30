package cn.dmsolr.zeppelin.spark;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkLauncher;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;

public class SparkJob extends AbstractJob {
	private final Props sysProps;
	private final Props jobProps;

	public SparkJob(String jobid, Props sysProps, Props jobProps, org.apache.log4j.Logger logger) {
		super(jobid, logger);
		this.sysProps = sysProps;
		this.jobProps = jobProps;
	}
	
	volatile boolean success = false; 
	volatile String applicationID = null;
	AtomicBoolean running = new AtomicBoolean(true);
	
	@Override
	public void run() throws Exception {
		String diagnosticsInfo = "";

		// for yarn rest api
		YarnAPI yarnRestApi = new YarnAPI(sysProps.getString("yarn.timeline.host", "localhost"), sysProps.getInt("yarn.timeline.port", 8188));
		try {
			if(!yarnRestApi.ping()) 
				throw new Exception("connection to yarn timeline server failure....");
			
			jobProps.put("spark.jar", toAbsolutePath(jobProps.get("spark.jar")));
			jobProps.put("spark.properties.file", toAbsolutePath(jobProps.get("spark.properties.file")));
			
			SparkLauncher launcher = new SparkLauncher()
					.setPropertiesFile(jobProps.get("spark.properties.file"))
					.setMainClass(jobProps.get("spark.main.class"))
					.setAppResource(jobProps.get("spark.jar"))
					.setSparkHome(jobProps.get("spark.home"))
					.setDeployMode(jobProps.getString("spark.mode", "cluster"))
//					.setVerbose(jobProps.getBoolean("spark.verbose", false))
					.setMaster(jobProps.getString("spark.master", "yarn"))
					;
			
			launcher.setAppName(jobProps.getString("spark.app.name", jobProps.get("spark.main.class")));
			String files = jobProps.get("spark.files");
			if(!Strings.isNullOrEmpty(files)) {
				String[] split = files.split("\\,");
				StringBuffer buffer = new StringBuffer();
				for(String f : split) {
					buffer.append(toAbsolutePath(f)).append(",");
				}
				buffer.deleteCharAt(buffer.length()-1);
				launcher.addFile(buffer.toString());
			}
			
			String pref = "sparkconf.";
			final int prefLen = pref.length();
			Map<String, String> sparkConf = Maps.newHashMap();
			jobProps.toProperties().forEach((k, v) -> {
				String key = String.valueOf(k);
				if(key.startsWith(pref)) {
					sparkConf.put(key.substring(prefLen), String.valueOf(v));
				}
			});
			sparkConf.forEach((k, v) -> {
				k = "--" + k;
				if(Strings.isNullOrEmpty(v))
					launcher.addSparkArg(k);
				else 
					launcher.addSparkArg(k, v);		
			});
			String arguments = jobProps.getString("spark.app.arguments", null);
			if(!Strings.isNullOrEmpty(arguments)) {
				if(!arguments.contains(" "))
					launcher.addAppArgs(arguments);
				else 
					launcher.addAppArgs(arguments.split("\\s+"));
			}
			
			// sparkHandler.Listener can't obtain the appid
//			SparkAppHandle startApplication = 
			launcher.startApplication(new Listener() {
				@Override public void infoChanged(SparkAppHandle handle) {
					String appid = handle.getAppId();
					if(!Strings.isNullOrEmpty(appid)) {
						if (Strings.isNullOrEmpty(applicationID))
							applicationID = appid;
						else if (!applicationID.equals(appid)) {
							info(String.format("[appid modified] last.appid = %s, cur.appid = %s", applicationID, appid));
						}
					}
					info(String.format("[info changed]state = %s, appid = %s", handle.getState().name(), appid));
				}
				@Override public void stateChanged(SparkAppHandle handle) {
					info(String.format("[state changed]state = %s, appid = %s", handle.getState().name(), handle.getAppId()));
					
					switch(handle.getState()) {
					case CONNECTED: 
					case UNKNOWN: 
					case RUNNING : 
						break;
					
					case FAILED : 
					case KILLED :
					case LOST : {
						warn("FAILED FAILED FAILED....");
						running.set(false);
						success = false;
						break;
					}
					case FINISHED : {
						warn("daming daming daming....");
						running.set(false);
						success = true;
						break;
					}
					default : break;
					}
				}
			});
			
			while(running.get()) try {
				TimeUnit.SECONDS.sleep(1);
			} catch (Exception e) {}
		} catch (Exception e) {
			error(e.getMessage(), e);
			throw e;
		} finally {
		}
		try {
			String response = yarnRestApi.getResponse(applicationID);
			JSONObject json = JSONObject.parseObject(response);
			
			String appState = json.getString("finalAppStatus");
			if (appState.equals("SUCCEEDED")) {
				info("success");
				success = true;
			}
			else {
				diagnosticsInfo = json.getString("diagnosticsInfo");
				error(diagnosticsInfo);
			}
			info(JSONObject.toJSONString(json, true));
		} catch (Exception e) {
			error(e.getMessage(), e);
		}
		if (!success)
			throw new Exception("failure");
	}
	
//	String applicationID = null;
//	void setAppID(String applicationID) {
//		this.applicationID = applicationID;
//	}

	final String toAbsolutePath(String path) {
		File f = new File(path);
		if (f.exists()) {
			return f.getAbsolutePath();
		} 
		f = new File(jobProps.get("working.dir"), path);
		return f.getAbsolutePath();
	}
	
	// application_1527062879361_0018
//	static final Pattern pattern = Pattern.compile("\\d+/\\d+/\\d+ \\d+:\\d+:\\d+ INFO Client: Application report for (application_\\d+_\\d+) \\(state: (\\w+)\\)");
//	static final String extraAppId(String text) {
//		Matcher matcher = pattern.matcher(text);
//		if(matcher.find()) 
//			return matcher.group(1);
//		return null;
//	}
//	
//	void printInfo(Process launch) {
//		new Thread(() -> {
//			try {
//				byte[] bb = new byte[256];
//				InputStream stream = launch.getInputStream();
//				while(true) {
//					int len = stream.read(bb);
//					if(len < 0) break;
//				}
//			} catch (Exception e) {}
//		}).start();
//	}
//	void printError(Process launch) {
//		new Thread(() -> {
//			try {
//				byte[] bb = new byte[256];
//				InputStream stream = launch.getErrorStream();
//				while(true) {
//					int len = stream.read(bb);
//					if(len < 0) break;
//					error(new String(bb, 0, len));
//				}
//			} catch (Exception e) { e.printStackTrace(); }
//		}).start();
//	}
}
