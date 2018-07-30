package cn.dmsolr.zeppelin.spark;

import java.io.IOException;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpHost;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;

import com.alibaba.fastjson.JSONObject;

public class YarnAPI {
	private HttpClient client = null;
	private HostConfiguration hostConf = new HostConfiguration();
	
	public YarnAPI(String host, int port) {
		HttpClientParams params = new HttpClientParams();
		HttpConnectionManager manager = new SimpleHttpConnectionManager();
		client = new HttpClient(params, manager);
		hostConf.setHost(new HttpHost(host, port));
	}
	
	public final String getStatus(String appid) throws HttpException, IOException {
		JSONObject result = getResponseInt(appid);
		return result.getString("appState");
	}
	
	public final String getResponse(String appid) throws HttpException, IOException {
		return JSONObject.toJSONString(getResponseInt(appid), true);
	}
	
	public final JSONObject getResponseInt(String appid) throws HttpException, IOException {
		GetMethod method = new GetMethod("/ws/v1/applicationhistory/apps/" + appid);
//		int status = 
				client.executeMethod(hostConf, method);
		
		return JSONObject.parseObject(method.getResponseBodyAsString());
	}
	
	public boolean ping() {
		GetMethod ping = new GetMethod("/ws/v1/applicationhistory/about");
		try {
			int status = client.executeMethod(ping);
			if (status != 200) 
				return false;
		} catch (Exception e) { }
		ping.abort();
		return true;
	}
}
