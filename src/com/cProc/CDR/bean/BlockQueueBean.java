package com.cProc.CDR.bean;

import javax.servlet.ServletContext;

import org.apache.hadoop.conf.Configuration;

/** 
 * @author yangzhenyu
 * @date 2012 3 29 14:28:02
 * @version v1.0 

 * @TODO 阻塞队列实体
 */

public class BlockQueueBean implements Comparable {

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String sessionID;//sessionID
	private int num;//读取文件数
	private ServletContext context;//servletcontext对象
	private Configuration conf;//conf配置对象
	
	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public ServletContext getContext() {
		return context;
	}

	public void setContext(ServletContext context) {
		this.context = context;
	}

	public String getSessionID() {
		return sessionID;
	}

	public void setSessionID(String sessionID) {
		this.sessionID = sessionID;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

}
