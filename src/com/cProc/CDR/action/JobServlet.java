package com.cProc.CDR.action;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.job.tools.getHostIP;
import org.apache.hadoop.net.NetUtils;
import org.cProc.GernralRead.ReadData.ParseIndexFormat;
import org.cProc.GernralRead.TableAndIndexTool.TableAndIndexTool;
import org.cProc.sql.IndexChooser;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.Job;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntityType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import protocol.json.JsonRequest;

import com.cProc.CDR.bean.BlockQueueBean;
import com.cProc.CDR.bean.ServletBean;
import com.cProc.CDR.util.ChooseIndexUtil;
import com.dinglicom.decode.util.CDRFieldUtil;

/** 
 * @author yangzhenyu
 * @date 2012 03 29 14:27:02
 * @version v2.1 

 * @TODO JobServlet 业务交互
 */

public class JobServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	public static final Log LOG = LogFactory.getLog(JobServlet.class.getName());
	
	/**
	 * 全局变量 
	 */	
	public static final Map<String, ServletBean> beans = new HashMap<String, ServletBean>();
	
	/**
	 * 预读文件存储变量
	 */
	public static final Map<String,String> datafile = new HashMap<String,String>();
	
	/**
	 * hadoop配置文件
	 */
	private static Configuration conf ;
	
	/**
	 * FS.properties配置文件对象
	 */
	private static final Properties properties ;
	
	/**
	 * init.properties配置文件对象
	 */
	private static final Properties initproperties ;
	
	/**
	 * 查询表结构Map
	 */
	private static final Map<String,List<String>> TABLE_MAP = new HashMap<String,List<String>>();
	
	/**
	 * 预读文件个数
	 */
	private static int READ_FILE_NUM;
	
	/**
	 * 预读文件Map
	 */
	public static final Map<String,Integer> READ_FILE_MAP = new HashMap<String, Integer>();
	
	/**
	 * 结果文件存放目录
	 */
	private static String tempResultDir;
	
	/**
	 * 结果文件（个数）存放目录
	 */
	private static String tempNotifyDir;
	
	/**
	 * 执行任务阻塞队列（与DataFileThreadTwo线程关联）
	 */
	private static BlockingQueue<BlockQueueBean> bq = new ArrayBlockingQueue<BlockQueueBean>(1024*10);
	
	/**
	 * 查询框架超时配置
	 */
	private static String timeout_times;
	
	/**
	 * jetty框架超时配置
	 */
	private static long timeout = 120000;
	
	/**
	 * dataIO_hostIP信息配置
	 */
	private static String dataIO_hostIP ;//wzt??? dataIO service 是什么进程,jetty 对应dataIO的ip
	
	/**
	 * FS_hostIP信息配置
	 */
	private static String FS_hostIP ;//hdfs master 主机address

	/**
	 * FS_hdfsIP信息配置
	 */
	private static String FS_hdfsIP ;//这个和上面的的FS_hostIP有何区别
	
	/**
	 * 读取数据超时容器
	 */
	public static Map<String,Long> READ_FILE_TIME = new HashMap<String,Long>();
	
	private static final Configuration cof = new Configuration();
	
	/*
	 * 加载配置文件，读取配置信息
	 */
	static{
		conf = new Configuration();
		properties = new Properties();
		initproperties = new Properties();
		try {
			
			/*
			 * 获取JobServlet.class目录下FS.properties配置文件 
			 */
			properties.load(JobServlet.class.getClassLoader().getResourceAsStream("com/cProc/CDR/action/FS.properties"));
			String initurl = properties.getProperty("initurl");
			//读取initurl配置信息
			initproperties.load(new FileReader(initurl));
			//获取结果文件存放目录
			tempResultDir = initproperties.getProperty("tempResultDir");
			//结果文件（个数）存放目录
	        tempNotifyDir = initproperties.getProperty("tempNotifyDir");
	        //获取查询框架超时配置
	        timeout_times = properties.getProperty("timeout_times");
	        //jetty超时配置
	        timeout = Long.parseLong(properties.getProperty("timeout"));
	        //dataIO_hostIP信息
	        dataIO_hostIP = initproperties.getProperty("hostIp");
	        //FS_hostIP  这2个IP分别用来干嘛用 ？？？
	        FS_hostIP = properties.getProperty("namenodeIp");
	        //FS_hdfsIP
	        FS_hdfsIP = properties.getProperty("FS");
	        //获取读取文件个数配置
	        READ_FILE_NUM = Integer.parseInt(initproperties.getProperty("read_file_num"));
	        if(timeout_times == null || timeout_times.equals(""))
	        	timeout_times = "20";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.info("properties load error ! ");
			e.printStackTrace();
		}
		
		/*
		 * 初始化删除文件线程
		 */
//		CleanThread cleanThread = new CleanThread(tempResultDir,tempNotifyDir);
//		cleanThread.start();
		
		/*
		 * 初始化预读文件线程
		 */
		for(int i=0;i<60;i++){
			DataFileThreadTwo dft = new DataFileThreadTwo(bq,READ_FILE_NUM);
			Thread th = new Thread(dft);
			th.start();
		}
		
		//加载配置文件core-site
		conf.addResource(new Path(properties.getProperty("com/cProc/CDR/action/core-site")));
		
		// 初始化TABLE_MAP,存储表结构   ,表结构在那个目录下
		TABLE_MAP.put("BSSAP",getFileds("BSSAP"));
		TABLE_MAP.put("BICC",getFileds("BICC"));
		TABLE_MAP.put("IUCS",getFileds("IUCS"));
		TABLE_MAP.put("MAP",getFileds("MAP"));
		TABLE_MAP.put("CAP",getFileds("CAP"));
		TABLE_MAP.put("ABIS",getFileds("abiscdr"));
	}
	
	/*
	 * 解析json串并拼凑SQL语句
	 */
//	public static String getSql(String condition,String sessionId) {
//		String databaseName = conf.get("hadoop.database.name");
//		net.sf.json.JSONObject obj = net.sf.json.JSONObject.fromObject(condition);
//		String tableName = obj.getString("protocol").toLowerCase();
//		String sql = "select all from " + tableName;
//		sql += " on " + databaseName + " where ";
//		Set set = obj.entrySet();
//		Iterator it = set.iterator();
//		Boolean f = false;
//		String calling_number = "";
//		String called_number = "";
//		if(obj.get("called_number") != null && obj.get("calling_number") != null){
//			f = true;
//		}
//		String objStr = "";
//		List<String> list = TABLE_MAP.get(tableName.toUpperCase());
//		while (it.hasNext()) {
//			Map.Entry<String, Object> my = (Map.Entry<String, Object>) it.next();
//			String key = my.getKey().trim();
//			if(list.contains(key.toLowerCase()) || key.equals("protocol") || key.equals("time_range_list") || key.equals("pair_elem_list") || key.equals("ISDownCDR") || key.equals("Where")){
//				if (key.equalsIgnoreCase("time_range_list")) {
//					String value = my.getValue().toString();
//					net.sf.json.JSONArray array =net.sf.json. JSONArray.fromObject(value);
//					Iterator iterator = array.iterator();
//					sql += "(";
//					while(iterator.hasNext()){
//						String str = iterator.next().toString();
//						net.sf.json.JSONObject obj1 = net.sf.json.JSONObject.fromObject(str);
//						/*
//						 * ISDownCDR 参数为下钻参数 
//						 * CDR查询时改参数无效
//						 */
//						if(obj.get("ISDownCDR")!=null){
//							if(tableName.equalsIgnoreCase("ABIS")){
//								//下钻只匹配end_time_s参数
//								sql += "end_ts_s >= "+ obj1.getString("start_time_s") +" and end_ts_s<=" + obj1.getString("end_time_s") + " or  ";
//							}else{
//								//下钻只匹配end_time_s参数
//								sql += "end_time_s >= "+ obj1.getString("start_time_s") +" and end_time_s<=" + obj1.getString("end_time_s") + " or  ";
//							}
//						}else {
//							if(tableName.equalsIgnoreCase("ABIS")){
//								//CDR查询只匹配start_time_s参数
//								sql += "begin_ts_s>=" + obj1.getString("start_time_s")	+ " and begin_ts_s<=" + obj1.getString("end_time_s") + " or  ";
//							}else{
//								//CDR查询只匹配start_time_s参数
//								sql += "start_time_s>=" + obj1.getString("start_time_s")	+ " and start_time_s<=" + obj1.getString("end_time_s") + " or  ";
//							}
//						}
//					}
//					sql = sql.substring(0,sql.length() - 4);
//					sql += ") and ";
//					
//				}else if(key.equalsIgnoreCase("mscid")){
//					String value = my.getValue().toString();
//					if(!value.equals("[]")){
//						String mscid = value.substring(1,value.length()-1);
//						if(mscid.contains(",")){
//							sql += "mscid in ("+value.substring(1,value.length()-1)+") and ";
//						}else{
//							sql += "mscid == " + mscid + " and ";
//						}
//					}
//				}else if (key.equalsIgnoreCase("pair_elem_list")) {
//					net.sf.json.JSONArray array = obj.getJSONArray("pair_elem_list");				
//					Iterator iterator = array.iterator();
//					Boolean pairFlag = false;
//					if(iterator.hasNext()){
//						sql += "(";
//					}
//					while(iterator.hasNext()){
//						String str = iterator.next().toString();
//						net.sf.json.JSONObject obj1 = net.sf.json.JSONObject.fromObject(str);
//						sql += " (dpc==" + obj1.getString("dpc")	+ " and opc==" + obj1.getString("opc") + ") or  ";
//						pairFlag = true;
//					}
//					if(pairFlag){
//						sql = sql.substring(0,sql.length() - 4);
//						sql += ") and ";
//					}
//				}else if(!key.equalsIgnoreCase("protocol") && !key.equalsIgnoreCase("where") && !key.equalsIgnoreCase("ISDownCDR")){
//					String value = my.getValue().toString();
//					if(my.getKey().equals("calling_number") && f == true){
//						calling_number = value;
//						continue;
//					}
//					if(my.getKey().equals("called_number") && f == true){
//						called_number = value;
//						continue;
//					}
//					if(value.contains("[") && value.contains("]")){
//						value = value.replace("[", "");
//						value = value.replace("]", "");
//						if(!value.trim().equals("")){
//							if(value.contains(",")){
//								sql += " " +key + " in (" + value + ") and ";
//							}else{
//								sql += " " +key + "==" + value + " and ";
//							}
//						}
//					}else{
//						if(value.contains("?") || value.contains("*")){
//							sql += " " +key + " like " + value + " and ";
//						} else {
//							sql += " " +key + "==" + value + " and ";
//						}
//	
//					}
//					objStr += my.getKey().toString();
//					objStr += ",";
//				}else if(key.equalsIgnoreCase("where")){
//					String value = my.getValue().toString();
//					sql += " " + value + " and ";
//				}
//				my = null;
//			}else{
//				return null;
//			}
//		}
//		if(f){
//			sql += " (calling_number == "+calling_number+" or called_number == "+called_number+" ) ";
//		}else{
//			sql = sql.substring(0,sql.length() -4);
//		}
//		
//		databaseName = null;
//		obj = null;
//		set = null;
//		it = null;
//		return sql;
//
//	}
	//新版本自动选索引  有测试函数吗
	public static String getSql(String condition,String sessionId) {
		//databasename 是什么
		//select all from bssap on cdr where (start_time_s>=1388851200 and start_time_s<=1388887200 ) and  calling_number==3905182067 and  cdr_type in (1,2,3,4,5,6,7,8,12,13,14,18,19) &&&callingIndex		
		//select all from bssap on cdr where  cdr_rel_type==12 and  last_mscid==653595 and (end_time_s >= 1389045600 and end_time_s<=1389049199 ) &&&drillIndex		
		String databaseName = conf.get("hadoop.database.name");
		net.sf.json.JSONObject obj = net.sf.json.JSONObject.fromObject(condition);
		String tableName = obj.getString("protocol").toLowerCase();
		String sql = "select all from " + tableName;
		sql += " on " + databaseName + " where ";
		Set set = obj.entrySet();
		Iterator it = set.iterator();
		Boolean f = false;
		String calling_number = "";
		String called_number = "";
		if(obj.get("called_number") != null && obj.get("calling_number") != null){
			f = true;
		}
		//select all from bssap on cdr where 
		String objStr = "";
		List<String> list = TABLE_MAP.get(tableName.toUpperCase());
		while (it.hasNext()) {
			Map.Entry<String, Object> my = (Map.Entry<String, Object>) it.next();
			String key = my.getKey().trim();
			if(list.contains(key.toLowerCase()) || key.equals("protocol") 
					|| key.equals("time_range_list") || key.equals("Where") 
					|| key.equals("ISDownCDR") ||  key.equals("pair_elem_list")){ 
				if (key.equalsIgnoreCase("time_range_list")) {
					String value = my.getValue().toString();
					net.sf.json.JSONArray array =net.sf.json. JSONArray.fromObject(value);
					Iterator iterator = array.iterator();
					sql += "(";
					while(iterator.hasNext()){
						String str = iterator.next().toString();
						net.sf.json.JSONObject obj1 = net.sf.json.JSONObject.fromObject(str);
						/*
						 * ISDownCDR 参数为下钻参数 
						 * CDR查询时改参数无效
						 */
						if(obj.get("ISDownCDR")!=null){//下钻
							if(tableName.equalsIgnoreCase("ABIS")){
								//下钻只匹配end_time_s参数
								sql += "end_ts_s >= "+ obj1.getString("start_time_s") +" and end_ts_s<=" 
								+ obj1.getString("end_time_s") + " or  ";
							}else{
								//下钻只匹配end_time_s参数
								sql += "end_time_s >= "+ obj1.getString("start_time_s") +" and end_time_s<=" 
								+ obj1.getString("end_time_s") + " or  ";
							}
						}else {//普通查询
							if(tableName.equalsIgnoreCase("ABIS")){
								//CDR查询只匹配start_time_s参数
								sql += "begin_ts_s>=" + obj1.getString("start_time_s")	+ " and begin_ts_s<=" + obj1.getString("end_time_s") + " or  ";
							}else{
								//CDR查询只匹配start_time_s参数
								sql += "start_time_s>=" + obj1.getString("start_time_s")	+ " and start_time_s<=" + obj1.getString("end_time_s") + " or  ";
							}
						}
					}
					sql = sql.substring(0,sql.length() - 4);
					sql += ") and ";
					
				}else if(key.equalsIgnoreCase("mscid")){//mscid  采用 [] ,不采用json串
					String value = my.getValue().toString();
					if(!value.equals("[]")){
						String mscid = value.substring(1,value.length()-1);
						if(mscid.contains(",")){
							sql += "mscid in ("+value.substring(1,value.length()-1)+") and ";
						}else{
							sql += "mscid == " + mscid + " and ";
						}
					}
				}else if (key.equalsIgnoreCase("pair_elem_list")) {
					//解析字串里面的JSON
					net.sf.json.JSONArray array = obj.getJSONArray("pair_elem_list");				
					Iterator iterator = array.iterator();
					Boolean pairFlag = false;
					if(iterator.hasNext()){
						sql += "(";
					}
					while(iterator.hasNext()){
						String str = iterator.next().toString();
						net.sf.json.JSONObject obj1 = net.sf.json.JSONObject.fromObject(str);
						sql += " (dpc==" + obj1.getString("dpc")	+ " and opc==" + obj1.getString("opc") + ") or  ";
						pairFlag = true;
					}
					if(pairFlag){
						sql = sql.substring(0,sql.length() - 4);
						sql += ") and ";
					}
				}else if(!key.equalsIgnoreCase("protocol") && !key.equalsIgnoreCase("where") && 
						!key.equalsIgnoreCase("ISDownCDR")){
					String value = my.getValue().toString();
					if(my.getKey().equals("calling_number") && f == true){
						calling_number = value;
						continue;
					}
					if(my.getKey().equals("called_number") && f == true){
						called_number = value;
						continue;
					}
					if(value.contains("[") && value.contains("]")){
						value = value.replace("[", "");
						value = value.replace("]", "");
						if(!value.trim().equals("")){
							if(value.contains(",")){
								sql += " " +key + " in (" + value + ") and ";
							}else{
								sql += " " +key + "==" + value + " and ";
							}
						}
					}else{
//						if(value.contains("?")){
//							
//						}else if (value.contains("*")){
//							if(my.getKey().equals("calling_number")){
//								sql += " calling_number_str like '" + value.replace("*","%") + "' and ";
//							}else if(my.getKey().equals("called_number")){
//								sql += " called_number_str like '" + value.replace("*","%") + "' and ";
//							}else{
//								return null;
//							}
////							sql += " " +key + " like '" + value.replace("*","%") + "' and ";
//						} else {
//							if(my.getKey().equals("calling_number") && value.startsWith("0")){
//								sql += " calling_number_str=='" + value + "' and ";
//							}else if(my.getKey().equals("called_number") && value.startsWith("0")){
//								sql += " called_number_str=='" + value + "' and ";
//							}else{
//								sql += " " +key + "==" + value + " and ";
//							}
//						}
						
						if(value.contains("*") || value.contains("?")){
							return null;
						}else{
						
						//仅支持*或？，赞不能全部支持
						if(value.contains("*") && !value.contains("?")){
							if(my.getKey().equals("calling_number")){
								sql += " calling_number_str like '" + value.replace("*","%") + "' and ";
							}else if(my.getKey().equals("called_number")){
								sql += " called_number_str like '" + value.replace("*","%") + "' and ";
							}else{
								return null;
							}
						}else if (value.contains("?") && !value.contains("*")){
//							if(value.startsWith("?")){
//								if(value.length()>1){
//									sql += " "+key+" >= "+Integer.parseInt(value.replace("?","0"))+" and "+key+" <= 9" + value.substring(1).replace("?","9") + " and ";
//								}else{
//									sql += " "+key+" >= 0 and "+key+" <= 9 and ";
//								}
//							}else{
//								String replace = value;
//								sql += " "+key+" >= "+value.replace("?","0")+" and "+key+" <= " + replace.replace("?","9") + " and ";
//							}
							if(value.startsWith("?")){
								if(value.length()>1){
									if(my.getKey().equals("calling_number")){
										String replace = value;
										sql += " "+key+" >= "+Long.parseLong(value.replace("?","0"))+" and "+key+" <= 9" + value.substring(1).replace("?","9") + " and calling_number_str like '" + replace.replace("?","%") + "' and";
									}else if(my.getKey().equals("called_number")){
										String replace = value;
										sql += " "+key+" >= "+Long.parseLong(value.replace("?","0"))+" and "+key+" <= 9" + value.substring(1).replace("?","9") + " and called_number_str like '" + replace.replace("?","%") + "' and";
									}else{
										return null;
									}
								}else{
									return null;
								}
							}else{
								if(my.getKey().equals("calling_number")){
									String replace = value;
									String replace1 = value;
									sql += " "+key+" >= "+Long.parseLong(value.replace("?","0"))+" and "+key+" <= " + replace.replace("?","9") + " and calling_number_str like '" + replace1.replace("?","%") + "' and";
								}else if(my.getKey().equals("called_number")){
									String replace = value;
									String replace1 = value;
									sql += " "+key+" >= "+Long.parseLong(value.replace("?","0"))+" and "+key+" <= " + replace.replace("?","9") + " and called_number_str like '" + replace1.replace("?","%") + "' and";
								}else{
									return null;
								}
							}
						}else {
							sql += " " +key + "==" + value + " and ";
						}
						}
					}
					objStr += my.getKey().toString();
					objStr += ",";
				}else if(key.equalsIgnoreCase("where")){
					String value = my.getValue().toString();
					sql += " " + value + " and ";
					objStr = "ISDownCDR,";
				}
				my = null;
			}else{
				return null;
			}
		}
		if(f){
			sql += " (calling_number == "+calling_number+" or called_number == "+called_number+" ) ";
		}else{
			sql = sql.substring(0,sql.length() -4);
		}
		//下面时索引选择
		//select all from iucs on cdr where (start_time_s >= 1388737600 and start_time_s <= 1388973600 )
		//and called_number==5261838155 and cdr_type in (1,2,3,4,5,6,7,8,12,13,14,18,19)  &&&calledIndex
		//select all from iucs on cdr where (start_time_s >= 1388737600 and start_time_s <= 1388973600 )
		//and calling_number==5261838155 and cdr_type in (1,2,3,4,5,6,7,8,12,13,14,18,19)  &&&callingIndex

		if(!objStr.contains("ISDownCDR")){
			if(tableName.equals("abis")){
				tableName = "abiscdr";
			}
			ChooseIndexUtil chooseIndexUtil = new ChooseIndexUtil();
			if(objStr.equals("")){
				if(tableName.equals("abiscdr")){
					objStr = "begin_ts_s";
				}else{
					objStr = "start_time_s";
				}
			}
			String indexName = chooseIndexUtil.getIndexLoader(objStr.substring(0,objStr.length()-1),tableName);
			LOG.info("indexName is " + indexName);
			if(sql!=null){
				sql += "&&&"+indexName;
			}
		}else{
			if(sql.contains("imsi") && (sql.contains("iucs") || sql.contains("bssap"))){
				sql += "&&&imsiDrillIndex";
			}else{
				sql += "&&&drillIndex";
			}
		}
				
		databaseName = null;
		obj = null;
		set = null;
		it = null;
		return sql;

	}
	
	public static void main(String[] args) throws HttpException, IOException{
		String jsonStr ="{\"command\":\"GET\", \"session\":\"aedcdf77-fa4c-4340-82c4-0dda236dded4\", \"condition\":{\"result\":[2,1,3,4],\"protocol\":\"ABIS\",\"time_range_list\":[{\"start_time_s\":1337580994,\"end_time_s\":1337584594}],\"type\":[1,17,2],\"MSISDN\":\"13811443431\",\"begin_ci\":[1342815725,1342815726,1342815731,1342815732]}}";
		net.sf.json.JSONObject jsonObject = new net.sf.json.JSONObject();
		jsonObject = net.sf.json.JSONObject.fromObject(jsonStr);
		System.out.println(jsonObject.get("command"));
//		JsonRequest jsonRequest = new JsonRequest();
//		jsonRequest.unpack(jsonStr);
//		JSONObject jsonObject = (JSONObject)JSONValue.parse(jsonRequest.getBody());
//		jsonObject = (JSONObject) jsonObject.get("condition");
//		System.out.println(jsonObject.toString());
//		getSql(jsonObject.toString());
//		
//		String time = "1328405530000";
		Date date = new Date();
		date.setTime(1334624400000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(date));
		
		List jastr = CDRFieldUtil.getCDRField("MAP");
		System.out.println(jastr);
		
//		while(true){
//			BufferedReader reader;
//			reader = new BufferedReader(new FileReader("Q:/1"));
//			String str = null;
////			net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
//			List list = new ArrayList();
//			if((str = reader.readLine())!=null){
//				if(str.equals("")){
//					try {
//						Thread.sleep(100);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}else{
//					System.out.println(TABLE_MAP.get("BSSAP"));
//					System.out.println(str);
//				}
//				net.sf.json.JSONArray jrows = new net.sf.json.JSONArray();
//				str = str.substring(1,str.length()-1);
//				String[] jStr = str.trim().split(",");
//				List list1 = new ArrayList();
//				for(int k = 0;k<jStr.length;k++){
//					list1.add(jStr[k]);
//			    }
//				list.add(list1);
//				for(int k = 0;k<jStr.length;k++){
//					jrows.add(jStr[k]);
//			    }
//				jRows.add(jrows);
//				System.out.println(jRows.toString());
//			}
//			JSON.toJSONString(list);
//			System.out.println(JSON.toJSONString(list));
//			reader.close();
//			try {
//				Thread.sleep(50);
//				System.out.println("sleep!");
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		File file = new File("Q:/aaaa.txt");
		System.out.println(file.lastModified());
		datafile.put("1","1");
		datafile.put("2","2");
		Iterator iterator = datafile.keySet().iterator(); 
		while(iterator.hasNext()) {
			Object oj = iterator.next();
//			datafile.get(oj).toString();
			  // key = oj.toString() 
			  // value = default_config.get(oj).toString()

			LOG.info(oj.toString());
		}
	}
	
	private void requestJson(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException, InterruptedException{
		//一下这一段时读取request 请求串
		String initurl = properties.getProperty("TableAndIndexTool");
		if(initurl.equals("")){
			initurl = "";
		}
		//这里 ？？？
		TableAndIndexTool.setPrePath(initurl);
		String tempFilePath = "/result";
		InputStream input = request.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String str = null;
		String jsonStr = "";
		while((str = reader.readLine())!=null){
			jsonStr+=str;
		}
		reader.close();
		//解析请求串
	    LOG.info("request josn str :"+jsonStr);
//	    String condition ="{\"command\":\"GET\", \"session\":\""+sessionId+"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1331603329,\"end_time_s\":1331604736}],\"called_number\":\"10086\"}}";
		net.sf.json.JSONObject joa = new net.sf.json.JSONObject();
		joa = net.sf.json.JSONObject.fromObject(jsonStr);
		if(joa.get("command").equals("BYE")){
			//如果是结束命令
			String sid = joa.get("session").toString();
			LOG.info("====================================== bye ! sessionID : " + sid);
			beans.remove(sid);
			//删除给对方发送的文件记录
        	READ_FILE_MAP.remove(sid);
//        	requestPool.finishRequest(sid);
        	READ_FILE_TIME.remove(sid);
        	String jobid = request.getSession().getServletContext().getAttribute(sid).toString();
//        	ClientProtocol clientProtocolnamenode = null;
//    		InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr(FS_hostIP, 9000);
        	//rpc请求发停止job的命令
    		CProcFrameworkProtocol frameworkNode = null;
    		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(FS_hostIP, 8888);
    		try {
//    			clientProtocolnamenode = (ClientProtocol)RPC.getProxy(ClientProtocol.class,ClientProtocol.versionID, nameNodeAddr, conf,NetUtils.getSocketFactory(conf, ClientProtocol.class));
    			frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,	CProcFrameworkProtocol.versionID, frameworkNodeAddr, conf, NetUtils.getSocketFactory(conf, CProcFrameworkProtocol.class));
    			frameworkNode.stopJob(jobid);
    		}catch (Exception e){
    			e.printStackTrace();
    		}
//    		finally{
//    			RPC.stopProxy(frameworkNode);
//    		}
    		//删除结果文件目录，删除结果文件个数目录
        	if(DeleteDir(sid)){
        		LOG.info("delete Dir success ! ");
        	}else{
        		LOG.info("delete Dir error ! ");
        	}
        	request.getSession().getServletContext().removeAttribute(sid);
			return ;
		}
		//获取查询开始时间，结束时间
	    JsonRequest jsonRequest = new JsonRequest();
	    jsonRequest.unpack(jsonStr);
	    JSONObject jsonObject = (JSONObject)JSONValue.parse(jsonRequest.getBody());
	    jsonObject = (JSONObject) jsonObject.get("condition");
	    String tableName = jsonObject.get("protocol").toString();
		JSONArray jArray = (JSONArray)jsonObject.get("time_range_list");
		jsonObject = (JSONObject) jArray.get(0);
	    String startTime = jsonObject.get("start_time_s").toString();
	    String endTime = jsonObject.get("end_time_s").toString();
	    LOG.info("starttime is : " + startTime);
	    LOG.info("endtime is : " + endTime);
	    String sessionId =jsonRequest.getSession();
		tempFilePath += "/"+sessionId+"/";
		File resultDir = new File(tempFilePath);
		if(!resultDir.exists()){
			resultDir.mkdirs();
		}
	    String command =  jsonRequest.getCommand();
//	    String sql ="";
	    String condition = jsonRequest.getCondition().toString();
//        sql =getSql(condition);
//        LOG.info("sql :"+sql);
//        if(sql == null){
//        	return;
//        }
	    //
        String hdfsPath = conf.get("fs.default.name");
        String hadoopPath = conf.get("hadoop.install.dir");
	    String nameNode =  conf.get("hadoop.usrjar.ip");
	    nameNode = nameNode.substring(0,nameNode.lastIndexOf(":")+1)+FS_hdfsIP.split(":")[2];
//	    nameNode = "172.3.2.103:9000";
	    //	    DataFileThread dfThread =null;
        if(command.equals("GET")){
        	
//        	String propertiesutil = PropertiesUtil.getNameIp();
//    		String[] temp = propertiesutil.split(",");
        	//createJob里面会创建SQL ???? 最终要
    		Job job = createJob(hadoopPath+"CDR_ChinaMobile.jar","org.apache.hadoop.hdfs.job.framework.DoNNDIR"
    				,getHostIP.getLocalIP(),sessionId,hdfsPath
    				,new Text("CDR_Search"),startTime,endTime,condition);
    		if(job == null){
    			return;
    		}
    		String jobId = job.getJobId().toString();
            request.getSession().getServletContext().setAttribute(sessionId,jobId);
            LOG.info("see log jobId : "+jobId);
//    		ClientProtocol clientProtocol = null;
            //这2个IP ？？
    		LOG.info(FS_hdfsIP);
    		LOG.info(FS_hostIP);
//    		InetSocketAddress nameNodeAddr = NetUtils.createSocketAddr(FS_hostIP, 9000);
    		CProcFrameworkProtocol frameworkNode = null;
    		
    		InetSocketAddress frameworkNodeAddr = NetUtils.createSocketAddr(FS_hostIP, 8888);
    		try {
//    			clientProtocol = (ClientProtocol)RPC.getProxy(ClientProtocol.class,ClientProtocol.versionID, nameNodeAddr, conf,NetUtils.getSocketFactory(conf, ClientProtocol.class));
    			frameworkNode = (CProcFrameworkProtocol)RPC.getProxy(CProcFrameworkProtocol.class,	CProcFrameworkProtocol.versionID, frameworkNodeAddr, conf, NetUtils.getSocketFactory(conf, CProcFrameworkProtocol.class));
    			frameworkNode.changeJobEntityType(String.valueOf(jobId) , JobEntityType.RUNNING);
    			frameworkNode.submitJob(job);
    			
        	    ServletBean bean = new ServletBean();
        	    bean.setSessionId(sessionId);
    		    JobMonitor jobMonitor = new JobMonitor();
    		    bean = jobMonitor.MonitorLocalDir(bean.getSessionId(), bean, conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext(),conf,timeout);
    		    if(bean == null){//bean == null 时神马情况，无数据的状态
    		    	LOG.info("========================SEND END=======================");
    		    	BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
                    net.sf.json.JSONObject jo = new net.sf.json.JSONObject();
                    net.sf.json.JSONObject jdata = new net.sf.json.JSONObject();
                    net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
                    jdata.put("rows",jRows);
                    jdata.put("fields",TABLE_MAP.get(tableName));
                    jdata.put("table",tableName);
                    jdata.put("Total",0);
                    jo.put("data",jdata);
                    jo.put("code_message","OK");
                    jo.put("code","200");
                    LOG.info("begin to send to client , sessionId is : " + sessionId + " , jRows size is : " + jRows.size() +" , tableName is : " + tableName);
                    beans.remove(sessionId);
                    writer.write(jo.toString());
                    writer.newLine();
                    writer.flush();
                    writer.close();
                    LOG.info("end to send to client , sessionId is : " + sessionId);
                    if(DeleteDir(sessionId)){
                    	LOG.info("delete Dir success ! ");
                    }else{
                    	LOG.info("delete Dir error ! ");
                    }
                    jo = null;
                    jdata = null;
                    jRows = null;
                    request.getSession().getServletContext().removeAttribute(sessionId);
//                    requestPool.finishRequest(sessionId);
                    return ;
    		    }
    		    READ_FILE_MAP.put(sessionId,READ_FILE_NUM);
    		    beans.put(sessionId, bean);
    		    READ_FILE_TIME.put(sessionId,new Date().getTime());
//    		    dfThread = new DataFileThread(sessionId, bean,conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext(),conf);
//    		    dfThread.start();
    		    //BlockQueuBean 干什么用的，下面这一段是读取查询传回来的文件(文件已经放置在了磁盘上)
    		    BlockQueueBean bqo = new BlockQueueBean();
    		    bqo.setConf(conf);
    		    bqo.setContext(request.getSession().getServletContext());
    		    bqo.setNum(bean.getFileName());
    		    bqo.setSessionID(sessionId);
    		    bq.add(bqo);
    		    
//    		    THREAD_MAP.put(sessionId,dfThread);
    		    LOG.info("sessionId is : "+sessionId+" , local file name :"+bean.getLocalFileName());
    		    File file = new File(bean.getLocalFileName());
    		    reader = new BufferedReader(new FileReader(file));
                str = null;
//                net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
//                List<List<String>> jRows = new ArrayLisst<List<String>>();
                String jRows = "[]";
                if((str = reader.readLine())!=null){
//                	net.sf.json.JSONArray jrows = new net.sf.json.JSONArray();
//                	List<String> jrows = new ArrayList<String>();
//                	str = str.substring(1,str.length()-1);
//                	String[] jStr = str.split(",");
//                	for(int k = 0;k<jStr.length;k++){
//                		jrows.add(jStr[k]);
//                    }
//                	jRows.add(jrows);
//                	jrows = null;
                	jRows = str;
                }
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
                net.sf.json.JSONObject jo = new net.sf.json.JSONObject();
                net.sf.json.JSONObject jdata = new net.sf.json.JSONObject();
                jdata.put("rows",jRows);                
                jdata.put("fields",TABLE_MAP.get(tableName));
                jdata.put("table",tableName);
                jdata.put("Total",getTotal(sessionId,conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext()));
                jo.put("data",jdata);
                jo.put("code_message","OK");
                jo.put("code","100");
                LOG.info("begin to send to client , sessionId is : " + sessionId + " , tableName is : " + tableName);
//                LOG.info("begin to send to client , sessionId is : " + sessionId + " , jRows size is : " + jRows.size() +" , tableName is : " + tableName);
//                aaa.add(jRows.size());
                writer.write(jo.toString());
                writer.newLine();
                writer.flush();
                writer.close();
                LOG.info("end to send to client , sessionId is : " + sessionId);
                jo = null;
                jdata = null;
                jRows = null;
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
//    		finally{
//    			RPC.stopProxy(frameworkNode);
//    		}
        }else if(command.equals("CONTINUE")){
        	//下面的代码怎么没有看见磁盘文件的读写发送
        	ServletBean bean = beans.get(sessionId);
//        	LOG.info("==============================1111111111111111");
        	JobMonitor jobMonitor = new JobMonitor();
//        	String jobId = requestPool.getJobId(sessionId);
//        	if(jobId==null || jobId.equals("")){
//        		return;
//        	}
//        	request.getSession().getServletContext().setAttribute(sessionId,jobId);
  		    bean = jobMonitor.MonitorLocalDir(bean.getSessionId(), bean, conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext(),conf,timeout);
        	if(bean == null){
        		LOG.info("No data to client send End , sessionId is : " + sessionId);
		    	BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
                net.sf.json.JSONObject jo = new net.sf.json.JSONObject();
                net.sf.json.JSONObject jdata = new net.sf.json.JSONObject();
                net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
                jdata.put("rows",jRows);
                jdata.put("fields",TABLE_MAP.get(tableName));
                jdata.put("table",tableName);
//                int i = getTotal(sessionId,conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext()) ;
//                LOG.info("==============yzy---------------- total is : " + i);
                jdata.put("Total",0);
                jo.put("data",jdata);
                jo.put("code_message","OK");
                jo.put("code","200");
                beans.remove(sessionId);
                LOG.info("begin to send to client , sessionId is : " + sessionId + " , jRows size is : " + jRows.size() +" , tableName is : " + tableName);
//                int ttt = 0;
//                for(int ia = 0 ;ia<aaa.size();ia++){
//                	ttt = ttt + Integer.parseInt(aaa.get(ia).toString());
//                }
//                System.out.println(ttt);
//                aaa.clear();
                writer.write(jo.toString());
                writer.newLine();
                writer.flush();
                writer.close();
                LOG.info("end to send to client , sessionId is : " + sessionId);
//                datafile.clear();
                if(DeleteDir(sessionId)){
                	LOG.info("delete Dir success ! ");
                }else{
                	LOG.info("delete Dir error ! ");
                }
                jo = null;
                jdata = null;
                jRows = null;
//                THREAD_MAP.remove(sessionId);
                READ_FILE_MAP.remove(sessionId);
                request.getSession().getServletContext().removeAttribute(sessionId);
//                requestPool.finishRequest(sessionId);
                READ_FILE_TIME.remove(sessionId);
                return ;
        	}
        	synchronized(READ_FILE_MAP){
        		if(READ_FILE_MAP.containsKey(sessionId)){
        			int file_num = READ_FILE_MAP.get(sessionId);
        			READ_FILE_MAP.put(sessionId,file_num+READ_FILE_NUM);
        		}
        	}
//        	if(bean.getFileName()%3==1){
//	        	Thread df = null;
//	        	if(THREAD_MAP.get(sessionId)!=null){
//	        		df = (Thread)THREAD_MAP.get(sessionId);
//	        		if(df.isAlive()){
//	//        			LOG.info("df.isAlive : true ~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//	    	        	synchronized (df) {
//	    	        		try {
//	    	        			df.notify();
//	    					} catch (Exception e) {
//	    						e.printStackTrace();
//	    					}
//	    				}
//	            	}
//	        	}
//        	}
//			LOG.info("==============================22222222222222222" + bean.getFileName());
        	LOG.info("sessionId is : "+sessionId+" , local file name :"+bean.getLocalFileName());
//		    File file = new File(bean.getLocalFileName());
//		    reader = new BufferedReader(new FileReader(file));
//            str = null;
//            net.sf.json.JSONArray jRows = new net.sf.json.JSONArray();
//        	List<List<String>> jRows = new ArrayList<List<String>>();
        	String jRows = "[]";
        	//？？？ 返回数据
            jRows = getDataFile(sessionId+""+bean.getFileName());
//            while((str = reader.readLine())!=null){
//            	net.sf.json.JSONArray jrows = new net.sf.json.JSONArray();
//            	str = str.substring(1,str.length()-1);
//            	String[] jStr = str.split(",");
//            	for(int k = 0;k<jStr.length;k++){
//            		jrows.add(jStr[k]);
//                }
//            	jRows.add(jrows);
//            }
//            LOG.info("jRows.size() : "+jRows.size());
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(response.getOutputStream()));
            net.sf.json.JSONObject jo = new net.sf.json.JSONObject();
            net.sf.json.JSONObject jdata = new net.sf.json.JSONObject();
            jdata.put("rows",jRows);
            jdata.put("fields",TABLE_MAP.get(tableName));
            jdata.put("table",tableName);
            jdata.put("Total",getTotal(sessionId,conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext()));
//            LOG.info("@yzy==============================================getTotal : "+getTotal(sessionId,conf.get("hadoop.usrjar.ip"),request.getSession().getServletContext()));
            jo.put("data",jdata);
            jo.put("code_message","OK");
            jo.put("code","100");
            LOG.info("begin to send to client , sessionId is : " + sessionId + " , tableName is : " + tableName);
//            LOG.info("begin to send to client , sessionId is : " + sessionId + " , jRows size is : " + jRows.size() +" , tableName is : " + tableName);
            writer.write(jo.toString());
            writer.newLine();
            writer.flush();
            writer.close();
            LOG.info("end to send to client , sessionId is : " + sessionId);
            beans.remove(sessionId);
            beans.put(sessionId, bean);
            jo = null;
            jdata = null;
            jRows = null;
        }else{
        	beans.remove(sessionId);
        	READ_FILE_MAP.remove(sessionId);
//        	requestPool.finishRequest(sessionId);
        	READ_FILE_TIME.remove(sessionId);
        	if(DeleteDir(sessionId)){
        		LOG.info("delete Dir success ! ");
        	}else{
        		LOG.info("delete Dir error ! ");
        	}
        	request.getSession().getServletContext().removeAttribute(sessionId);
        	return ;
        }
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		this.doPost(request, response);
	}
	
	//这里时主入口
	public void doPost(HttpServletRequest request, HttpServletResponse response)throws ServletException, IOException {
		try {
			requestJson(request, response);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private  static Job createJob (String jar,String cclass,String localIP,String sessionId,String hdfsPath,Text jobName,String startTime,String endTime,String condition){
		//组合成SQL
		Configuration Myconf = new Configuration(false);
		String sql = getSql(condition,sessionId);
		LOG.info("sql :"+sql);
		if(sql == null){
			LOG.info("getSql() error !");
			return null;
		}
		//提交给查询框架的配置 ？？？
		Job job = new Job();
		LOG.info("====================yzy======================" + dataIO_hostIP);
		job.getConf().set("cProc.dataio.ip",dataIO_hostIP);
		job.getConf().set("hdfs.job.jar",jar);
		job.getConf().set("hdfs.job.class",cclass);
		job.getConf().set("hdfs.job.from.ip",localIP);
		job.setJobName(jobName);
		job.getConf().set("fs.default.name", hdfsPath);
		job.getConf().set("hdfs.job.param.sessionId",sessionId);
		job.getConf().set("cProc.starttime", startTime );
		job.getConf().set("cProc.endtime", endTime );
		job.getConf().set("timeout.times", timeout_times );
		
//		int  indexNum = 0;		
//		if(sql.contains("calling_number") || (sql.contains("calling") && sql.contains("abis"))){
//			indexNum = 0 ;//calling索引目录
//		}
//		if(sql.contains("called_number") && !sql.contains("calling_number") && !sql.contains("map") || (sql.contains("called") && sql.contains("abis") && !sql.contains("calling"))){
//			indexNum = 1;//called索引目录
//		}
//		String indexNameString = "" ;
//		if(!sql.contains("called_number") && !sql.contains("calling_number") && !sql.contains("call_number") && !sql.contains("calling") && !sql.contains("called")){
//			if(sql.contains("imsi")){
//				indexNameString = "imsiIndex";//imsi索引目录
//			}else if(sql.contains("imei")){
//				indexNameString = "imeiIndex";//imei索引目录
//			}else if(sql.contains("tmsi")){
//				indexNameString = "tmeiIndex";//imei索引目录
//			}else{
//				if(sql.contains("begin_ts_s") || sql.contains("start_time_s")){
//					indexNameString = "calledIndex";
//				}else{
//					indexNameString = "drillIndex";//下钻索引目录
//				}
//			}
//		}else{
//			indexNameString = IndexChooser.getIndexName(indexNum);//true called = 1
//		}
//		if(sql.contains("abis")){
//			if(sql.contains("calling_imei")){
//				indexNameString = "callingImeiIndex";
//			}else if (sql.contains("called_imei")){
//				indexNameString = "calledImeiIndex";
//			}else if (sql.contains("calling_imsi")){
//				indexNameString = "callingImsiIndex";
//			}else if (sql.contains("called_imsi")){
//				indexNameString = "calledImsiIndex";
//			}else if (sql.contains("calling")){
//				indexNameString = "callingIndex";
//			}else if (sql.contains("called")){
//				indexNameString = "calledIndex";
//			}else if (sql.contains("imsi")){
//				indexNameString = "imsiIndex";
//			}else if (sql.contains("imei")){
//				indexNameString = "imeiIndex";
//			}else{
//				if(sql.contains("begin_ts_s") || sql.contains("start_time_s")){
//					indexNameString = "calledIndex";
//				}else{
//					indexNameString = "drillIndex";
//				}
//			}
//			sql = sql.replace("abis","abiscdr");
//		}
//		if(sql.contains("udm")){
//			indexNameString = "testIndex";
//		}
//		IndexChooser a = new IndexChooser(sql);
//		String indexPath = TableAndIndexTool.getIndexFormat(a.getTableName(), indexNameString, a.getAppName());//索引文件
//		String indexDataPath = TableAndIndexTool.getIndexDataPath(a.getTableName(), indexNameString, a.getAppName());//数据文件目录
//		sql = a.getNewSql();
//		Boolean sourceFilter = false;
//		LOG.info("indexPath : " + indexPath);
//		try {
//			sourceFilter = a.isNeedFilterData(new ParseIndexFormat(indexPath,FileSystem.get(Myconf)));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	//	LOG.info(" sourceFilter =================================================== "+sourceFilter);
//		job.getConf().set("cProc.cdrSQL",sql);
//		job.getConf().set("hdfs.job.param.indexDir",indexDataPath);
//		job.getConf().set("cProc.sourceFilter",""+sourceFilter);
//		job.getConf().set("cProc.hdfs",conf.get("fs.default.name"));	
//		job.getConf().set("cProc.path",indexPath);
//		job.getConf().set("cProc.NN.ip", FS_hostIP);
//		if(sql.contains("abis")){
//			job.getConf().set("cProc.table.name", "abiscdr");
//		}else{
//			job.getConf().set("cProc.table.name", a.getTableName());
//		}
//		Myconf = null;
//		a = null;
//		sourceFilter = null;
//		return job;
		//怎么会有  &&& 符号呢
		String[] indexStr = sql.split("&&&");
		if(indexStr[0].contains("abis")){
			indexStr[0] = indexStr[0].replace("abis","abiscdr");
		}
		LOG.info("sql split is :" + indexStr[0]);
		//选择最有化的索引
		IndexChooser a = new IndexChooser(indexStr[0]);
		if(!a.isOk()){
			LOG.info("sql Indexchooser error !");
			return null;
		}
		
		//在文本中记录SQL语句
		try {
			WriteSQL(sql);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//索引格式化 ？？？,TableAndIndexTool 是什么类，下面的函数起什么作用
		String indexPath = TableAndIndexTool.getIndexFormat(a.getTableName(), indexStr[1], a.getAppName());//索引文件
		String indexDataPath = TableAndIndexTool.getIndexDataPath(a.getTableName(), indexStr[1], a.getAppName());//数据文件目录
		sql = a.getNewSql();
		LOG.info("new sql is : " + sql);
		Boolean sourceFilter = false;
		LOG.info("indexPath : " + indexPath);
		try {
			sourceFilter = a.isNeedFilterData(new ParseIndexFormat(indexPath,FileSystem.get(cof)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//这些参数的作用 ？？？
		LOG.info(" sourceFilter =================================================== "+sourceFilter);
		job.getConf().set("cProc.cdrSQL",sql);
		job.getConf().set("hdfs.job.param.indexDir",indexDataPath);
		job.getConf().set("cProc.sourceFilter",""+sourceFilter);
		job.getConf().set("cProc.hdfs",conf.get("fs.default.name"));
		job.getConf().set("hdfs",conf.get("fs.default.name"));
		job.getConf().set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem" );
		LOG.info("set hdfs : " +conf.get("fs.default.name") );
		job.getConf().set("cProc.path",indexPath);
		job.getConf().set("cProc.NN.ip", FS_hostIP);
		if(sql.contains("abis")){
			job.getConf().set("cProc.table.name", "abiscdr");
			LOG.info("~~~~~~~~~~~~~~~~"+job.getConf().get("cProc.table.name"));
		}else{
			job.getConf().set("cProc.table.name", a.getTableName());
			LOG.info(a.getTableName());
		}
		Myconf = null;
		a = null;
		sourceFilter = null;
		return job;
	}
	
	public static List<String> getFileds(String tablename){
		List<String> jastr = CDRFieldUtil.getCDRField(tablename);
		List<String> jaList = new ArrayList<String>();
		for(int i=0;i<jastr.size();i++){
			jaList.add(jastr.get(i));
		}
		return jaList;
	}
	
	//杨震宇修改 增加返回json串Total（总数）字段
	public int getTotal(String sessionID,String namenode,ServletContext context){
		int total = 0 ;
		JobMonitor jobMonitor = new JobMonitor();
		if(jobMonitor.getJobStatus(sessionID, namenode, context,conf)){
			try {
		        File file = new File(tempNotifyDir+"/"+sessionID);
		        if(file.exists()){
		        	File[] files = file.listFiles() ;		    		
		    		for (File f : files) {
		    			String fi = f.getName().split("_")[1];
		    			total = total + Integer.parseInt(fi.trim());	    			
		    		}
		    		LOG.info("########################################################################################sessionID :" + sessionID + " , Total is : " + total);
		    		return total;
		        }else {
		        	LOG.info("########################################################################################sessionID :" + sessionID + " , Total is : " + total);
		    		return 0;
				}		
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		LOG.info("########################################################################################sessionID :" + sessionID + " , Total is : " + total);
		return total;
	}
	
	public boolean DeleteDir(String sessionID){
		boolean f = false;
		try{
	        File file = new File(tempNotifyDir+"/"+sessionID);
	        DeleteDir(file);
	        file = new File(tempResultDir+"/"+sessionID);
	        f = DeleteDir(file);
		}catch (Exception e) {
			e.printStackTrace();
		}
		return f;
	}
	
	public boolean DeleteDir(File file){
		boolean f = false;
		if(file.exists()){//判断文件是否存在
        	if(file.isFile()){//判断是否是文件
        		file.delete();//delete()方法 你应该知道 是删除的意思;
            }else if(file.isDirectory()){//否则如果它是一个目录
            	File files[] = file.listFiles();//声明目录下所有的文件 files[];
            	for(int i=0;i<files.length;i++){//遍历目录下所有的文件
            		files[i].delete();//把每个文件 用这个方法进行迭代
            	} 
            } 
        	file.delete(); 
        	f = true;
        }
		return f;
	}
	
	public String getDataFile (String sessionID) throws InterruptedException{
//		List<List<String>> jArray = new ArrayList<List<String>>() ;
		LOG.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`~~~~~~~~~~getDataFile is : "+sessionID);
		String jArray = "";
		Date date = new Date();
		Long starDate = date.getTime();
		while(true){
			if(datafile.containsKey(sessionID)){
				jArray = datafile.get(sessionID);
				while(jArray == null){
					jArray = datafile.get(sessionID);
					if(jArray != null){
						LOG.info("datafile read !");
					}
				}
				datafile.remove(sessionID);
				break;
			}
			Date date1 = new Date();
			Long nowDate = date1.getTime();
			Long aLong = nowDate - starDate ;
			if(aLong>=timeout){
				LOG.info("@yzy=========================================================== getDataFile time out !");
				return "[]";
			}
		}
		return jArray;
	}
	
	public static void WriteSQL(String sql) throws IOException{
		File file = new File("/dinglicom/cproc/jetty/sql.txt");
		if(!file.exists()){
			file.createNewFile();
		}
		BufferedWriter output = new BufferedWriter(new FileWriter(file,true));
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		String out = df.format(new Date()) + "," + sql;// new Date()为获取当前系统时间
		output.write(out);
		output.write("\n");
		output.flush();
		output.close();
	}
}
