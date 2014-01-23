package com.cProc.CDR.util;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * HTTPCLIENT demo
 * 
 * @author jiafeng.zhang
 *
 */
public class HttpClientDemo {
//	private DefaultHttpClient httpClient;
	
	private HttpEntity entity ;

	public HttpClientDemo(String url, String searchInfo) {
		try{
			DefaultHttpClient httpClient = new DefaultHttpClient();
			// ����һ��POST����
			HttpPost httpPost = new HttpPost(url);
			DefaultHttpClient httpclient = new DefaultHttpClient();
			HttpPost httppost = new HttpPost(url);
			httppost.setEntity(new StringEntity(searchInfo));
			
			HttpResponse response = httpclient.execute(httppost);
			entity = response.getEntity();
		}catch(Exception e){
			
		}
		
	}
	
	public String getResponseContent(){
		try {
			return EntityUtils.toString(entity);
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
