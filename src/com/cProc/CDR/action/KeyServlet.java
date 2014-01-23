package com.cProc.CDR.action;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.cstor.bean.CDRProtocol;
import com.cstor.service.QueryService;
import com.dinglicom.decode.bean.CapCDR;
import com.dinglicom.decode.util.CDRFieldUtil;

public class KeyServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	public static final Log LOG = LogFactory.getLog(KeyServlet.class.getName());
	
	/**
	 * 查询表结构Map
	 */
	private static final Map<String,List<String>> TABLE_MAP = new HashMap<String,List<String>>();

	/**
	 * hadoop配置文件
	 */
	private static Configuration conf ;
	
	/**
	 * FS.properties配置文件对象
	 */
//	private static final Properties properties ;
	
	/**
	 * FS_hdfsIP信息配置
	 */
	private static String FS_hdfsIP ;
	
	static{
//		conf = new Configuration();
//		properties = new Properties();
//		try {
//			
//			/*
//			 * 获取JobServlet.class目录下FS.properties配置文件 
//			 */
//			properties.load(JobServlet.class.getClassLoader().getResourceAsStream("FS.properties"));
//			//FS_hdfsIP
//	        FS_hdfsIP = properties.getProperty("FS");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			LOG.info("properties load error ! ");
//			e.printStackTrace();
//		}
		
		TABLE_MAP.put("BSSAP",getFileds("BSSAP"));
		TABLE_MAP.put("BICC",getFileds("BICC"));
		TABLE_MAP.put("IUCS",getFileds("IUCS"));
		TABLE_MAP.put("MAP",getFileds("MAP"));
		TABLE_MAP.put("CAP",getFileds("CAP"));
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		try {
			requestJson(request, response);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void requestJson(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException, InterruptedException{
		InputStream input = request.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String str = "";
		String jsonStr = "";
		while((str = reader.readLine())!=null){
			jsonStr+=str;
		}
		reader.close();
	    LOG.info("request josn str :"+jsonStr);
		net.sf.json.JSONObject joa = new net.sf.json.JSONObject();
		joa = net.sf.json.JSONObject.fromObject(jsonStr);
		if(!joa.containsKey("netElem")){
			LOG.info("json error !");
			return;
		}
		
		//协议类型1.2 or 1.4
		String key = joa.get("netElem").toString();
		//时间范围集合
		net.sf.json.JSONArray array = joa.getJSONArray("timeRangeList");
		ArrayList<Long> startlist = new ArrayList<Long>();
		
		for(int i=0;i<array.size();i++){
			net.sf.json.JSONObject obj1 = array.getJSONObject(i);
			//开始时间
			String starttime = obj1.get("nBeginTime").toString();
			if(!starttime.equals("") && starttime != null){
				startlist.add(Long.parseLong(starttime));
			}
		}
//		//开始时间
//		String starttime = obj1.get("nBeginTime").toString();
//		//结束时间
//		String endtime = obj1.get("nEndTime").toString();
		//分析粒度
		String timeRank = joa.get("timeRank").toString();
		//文件名范围
		net.sf.json.JSONArray array1 = joa.getJSONArray("netElemIdList");
		String filename = array1.get(0).toString()+"_"+array1.get(1);
		//协议名称
		String tableName = joa.get("proType").toString();
		//排序字段名
		String sortName = joa.get("sort").toString();
		if(sortName.equals("")){
			sortName = "rrbe_time";
		}else{
			sortName = "rrbe_time";
		}
		
		//转换分析粒度
		int time_Rank = 0 ;
		if(!timeRank.equals("") && timeRank!=null){
			time_Rank = Integer.parseInt(timeRank);
		}
		
		
//		long start_time = 0;
//		long end_time = 0;
//		if(!starttime.equals("") && starttime != null){
//			start_time = Long.parseLong(starttime);
//		}
		
//		if(!endtime.equals("") && endtime != null){
//			end_time = Long.parseLong(endtime);
//		}
		
		//判断协议类型
		int cdrProtocol = 0;
		if(tableName.equalsIgnoreCase("BSSAP")){
			cdrProtocol =  CDRProtocol.BSSAP;
		}else if (tableName.equalsIgnoreCase("CAP")){
			cdrProtocol =  CDRProtocol.CAP;
		}else if (tableName.equalsIgnoreCase("IUCS")){
			cdrProtocol =  CDRProtocol.IUCS;
		}
		
		String dataStr = "[";
		QueryService queryService = new QueryService();
		for(int jlist=0;jlist<startlist.size();jlist++){
			LOG.info("startlist.get(jlist) : " + startlist.get(jlist) + " ,time_Rank/5 : " + time_Rank/5 + " ,cdrProtocol : " + cdrProtocol + " ,/smp/data_root : " + "/smp/data_root" + " ,filename : " + filename + " ,sortName " + sortName);
			String jdata = queryService.getData(startlist.get(jlist), time_Rank/5, cdrProtocol, "/smp/data_root",filename,sortName,null,0);
			LOG.info("jdata is : " +jdata);
			if(jdata!=null && !jdata.equals("") && !jdata.equals("null")){
				String jArray[] = jdata.split(";");
				jdata = "";
				for(int i=0;i<jArray.length;i++){
					if(jArray == null || jArray[i].equals("null")){
						break;
					}					
					jdata += "[" + jArray[i].toString() + "],";
				}
				LOG.info("jdata : " + jdata);
				dataStr +=  jdata;
			}
		}
		//判断是否有数据，没数据则直接返回
		if(!dataStr.equals("[")){
			dataStr = dataStr.substring(0,dataStr.length()-1) + "]";
		}else{
			dataStr = null;
		}
		
		LOG.info("send data is : " + dataStr);
		//返回数据
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
        net.sf.json.JSONObject jo = new net.sf.json.JSONObject();
        net.sf.json.JSONObject jdatas = new net.sf.json.JSONObject();
//            jdata = "[[1336837035,133028150,1336837046,326805210,3323994038,0,0,0,1308953,1307914,8613444202,-110,8613749296,-110,0,8613749296,8613444202,9430,13917,13827138396,,3827138396,3827138396,92,0,0,0,0,460007123270235,-42,-1,111,100,10,31,8613827121451,0,0,252],[1336837035,133028150,1336837046,326805210,3323994038,0,0,0,1308953,1307914,8613444202,-110,8613749296,-110,0,8613749296,8613444202,9430,13917,13827138396,,3827138396,3827138396,92,0,0,0,0,460007123270235,-42,-1,111,100,10,31,8613827121451,0,0,252],[1336837035,133028150,1336837046,326805210,3323994038,0,0,0,1308953,1307914,8613444202,-110,8613749296,-110,0,8613749296,8613444202,9430,13917,13827138396,,3827138396,3827138396,92,0,0,0,0,460007123270235,-42,-1,111,100,10,31,8613827121451,0,0,252]]";
        jdatas.put("rows",dataStr);
        jdatas.put("fields",TABLE_MAP.get(tableName));
        jdatas.put("table",tableName);
        jo.put("data",jdatas);
        jo.put("code_message","OK");
        jo.put("code","100");
        writer.write(jo.toString());
        writer.newLine();
        writer.flush();
        writer.close();
	}
	
	public static List<String> getFileds(String tablename){
		List<String> jastr = CDRFieldUtil.getCDRField(tablename);
		List<String> jaList = new ArrayList<String>();
		for(int i=0;i<jastr.size();i++){
			jaList.add(jastr.get(i));
		}
		return jaList;
	}
}
