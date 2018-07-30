package cn.dmsolr.zeppelin.hive;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hive.beeline.BeeLine;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import azkaban.flow.CommonJobProperties;
import azkaban.jobExecutor.ProcessJob;

/**
 * @author Teny ZH(coder.daming@gmail.com)
 */
public class DmHiveWrapper {
	
	public static void main(String[] args) throws Exception {
		Properties jobProps = loadAzkabanProps();

		List<String> hiveargs = Lists.newArrayList();
		hiveargs.add("-u");
		hiveargs.add(getProperty(jobProps, "db.url"));
		hiveargs.add("-n");
		hiveargs.add(getProperty(jobProps, "user.name", "daming.azkaban"));
		hiveargs.add("-p");
		hiveargs.add(getProperty(jobProps, "passowrd", "DUMMY"));
		hiveargs.add("-d");
		hiveargs.add(getProperty(jobProps, "driver.class", "org.apache.hive.jdbc.HiveDriver"));
		
		// hive conf
		Map<String, String> hiveConf = getHiveConf(jobProps);
		for(Map.Entry<String, String> e : hiveConf.entrySet()) {
			hiveargs.add("--hiveconf");
			hiveargs.add(e.getKey()+"="+e.getValue());
		}
		
		// params
		hiveargs.addAll(getParams(jobProps));
		
		// hive var
		Map<String, String> hivevar = getHiveVar(jobProps);
		for(Map.Entry<String, String> e : hivevar.entrySet()) {
			hiveargs.add("--hivevar");
			hiveargs.add(e.getKey()+"="+e.getValue());
		}
		
		String jobname = String.format("mapred.job.name=azk_%s_%s_%s.%s", 
				jobProps.getProperty(CommonJobProperties.PROJECT_NAME),
				jobProps.getProperty(CommonJobProperties.FLOW_ID),
				jobProps.getProperty(CommonJobProperties.EXEC_ID),
				jobProps.getProperty(CommonJobProperties.JOB_ATTEMPT)
			);
		hiveargs.add("--hiveconf");
		hiveargs.add(jobname);
		
		String script = jobProps.getProperty("script");
		String query = jobProps.getProperty("query");
		
		int scriptIndex = Strings.isNullOrEmpty(script)?0:1; 
		int queryIndex = Strings.isNullOrEmpty(query)?0:1;
		
		if ((scriptIndex + queryIndex) != 1) {
			throw new Exception("script or query cannot be empty!");
		}
		else {
			if(scriptIndex == 1) {
				hiveargs.add("-f");
				hiveargs.add(script);
			}
			else {
				hiveargs.add("-e");
				hiveargs.add(query);
			}
		}
		System.out.println("hive args=" + hiveargs.toString());
		System.out.println("hive.query=" + query);
		
		int res = -1;
		try (BeeLine beeline = new BeeLine()) {
			beeline.setErrorStream(System.err);
			beeline.setOutputStream(System.out);
			res = beeline.begin(hiveargs.toArray(new String[hiveargs.size()]), null);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			System.exit(res);
		}
	}
	
	public static Properties loadAzkabanProps() throws IOException, FileNotFoundException {
		String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
		System.out.println(propsFile);
		Properties props = new Properties();
		props.load(new BufferedReader(new FileReader(propsFile)));
		return props;
	}
	
	public static Map<String, String> getHiveConf(Properties props) {
		Enumeration<Object> keys = props.keys();
		Map<String, String> conf = Maps.newHashMap();
		while(keys.hasMoreElements()) {
			Object el = keys.nextElement();
			String key = el.toString();
			if(key.startsWith("hiveconf.")) {
				conf.put(key.substring(9), props.getProperty(key));
			}
		}
		return conf;
	}
	
	public static List<String> getParams(Properties props) {
		List<String> params = Lists.newArrayList();
		Enumeration<Object> keys = props.keys();
		while(keys.hasMoreElements()) {
			Object el = keys.nextElement();
			String key = el.toString();
			if(key.startsWith("params.")) {
				params.add(key.substring(8));
			}
		}
		return params;
	}
	
	public static Map<String, String> getHiveVar(Properties props) {
		Enumeration<Object> keys = props.keys();
		Map<String, String> conf = Maps.newHashMap();
		while(keys.hasMoreElements()) {
			Object el = keys.nextElement();
			String key = el.toString();
			if(key.startsWith("hivevar.")) {
				conf.put(key.substring(8), props.getProperty(key));
			}
		}
		return conf;
	}
	
	private static final String getProperty(Properties props, String key) {
		String val = props.getProperty(key);
		if(Strings.isNullOrEmpty(val))
			return System.getProperty(key);
		return val;
	}
	
	private static final String getProperty(Properties props, String key, String defaultValue) {
		String val = getProperty(props, key);
		if(Strings.isNullOrEmpty(val))
			return defaultValue;
		return val;
	}
}
