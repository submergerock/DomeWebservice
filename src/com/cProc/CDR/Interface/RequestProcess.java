package com.cProc.CDR.Interface;

import net.sf.json.JSONObject;

/** 
 * @author yangzhenyu
 * @date 2012 06 01 10:32:33
 * @version v1.0 

 * @TODO RequestProcess 业务交互api
 */

public interface RequestProcess {
	
	/*
	 * 请求接口api
	 * param：JSONObject
	 * return：JSONObject
	 * {"command":"GET", "session":"dbd507cc-13b2-430b-b2c4-775de3675a67", "condition":{"protocol":"UDM","MSISDN":[8618663625533],"time_range_list":[{"start_time":1339295237,"end_time":1339295237}]}}
	 */
	public JSONObject requestAccept(JSONObject jsonObject);
}
