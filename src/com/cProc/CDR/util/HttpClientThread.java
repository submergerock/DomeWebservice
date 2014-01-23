package com.cProc.CDR.util;


import java.util.Date;

import net.sf.json.JSONObject;


/**
 * 
 * @author jiafeng.zhang
 *
 */
public class HttpClientThread extends Thread {
	
	private String url;
	
	private String condition;
	private String sessionid;
	private String buffer1 = "";
	private int size = 0;
	
	
	HttpClientThread(String url ,String condition,String sessionid,String buffer1){
		this.url = url;
		this.condition = condition;
		this.sessionid = sessionid;
		this.buffer1 = buffer1;
	}
	
	@Override
	public void run() {
		try{
			Date start = new Date();
			String buffer = condition;
			HttpClientDemo response = new  HttpClientDemo(url, buffer.toString());
			String content = response.getResponseContent();
			JSONObject json = JSONObject.fromObject(content);
			int code = Integer.parseInt(json.getString("code"));
			while(code == 100){
				Date single_begin = new Date();
				response = new HttpClientDemo(url, buffer1.toString());
				content = response.getResponseContent();
				json = JSONObject.fromObject(content);
				code = Integer.parseInt(json.getString("code"));
				Date single_end = new Date();
				System.out.println("every time cost :"+(single_end.getTime()-single_begin.getTime())+"ms");
			}
			Date end = new Date();
			System.out.println("time cost :" + ( end.getTime() - start.getTime()) + " ms");
		}catch(Exception e){
			e.printStackTrace();
		}

	}

}
