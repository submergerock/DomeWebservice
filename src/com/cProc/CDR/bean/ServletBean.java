package com.cProc.CDR.bean;

/** 
 * @author yangzhenyu
 * @date 2011 11 28 13:45:02
 * @version v1.0 

 * @TODO servlet实体
 */
public class ServletBean {
	private String sessionId;//sessionId
	private String localFileName;//文件名称
	private int fileName;//文件数

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getLocalFileName() {
		return localFileName;
	}

	public void setLocalFileName(String localFileName) {
		this.localFileName = localFileName;
	}

	public int getFileName() {
		return fileName;
	}

	public void setFileName(int fileName) {
		this.fileName = fileName;
	}

}
