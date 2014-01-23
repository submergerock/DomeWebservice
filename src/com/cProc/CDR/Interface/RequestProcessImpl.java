package com.cProc.CDR.Interface;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.job.tools.getHostIP;
import org.cProc.GernralRead.ReadData.ParseIndexFormat;
import org.cProc.GernralRead.TableAndIndexTool.TableAndIndexTool;
import org.cProc.sql.IndexChooser;
import org.cstor.cproc.cloudComputingFramework.Job;

/** 
 * @author yangzhenyu
 * @date 2012 06 01 10:32:33
 * @version v1.0 

 * @TODO RequestProcessImpl 业务交互api
 */

public class RequestProcessImpl implements RequestProcess{
	
	public static final Log LOG = LogFactory.getLog(RequestProcessImpl.class.getName());
	/**
	 * hadoop配置文件
	 */
	private static Configuration conf ;
	
	/**
	 * 查询框架超时配置
	 */
	private static String timeout_times;
	
	static{
		conf = new Configuration();
		timeout_times = "30";
	}
	
	//{"command":"GET", "session":"dbd507cc-13b2-430b-b2c4-775de3675a67", "ip":"192.168.0.1","tablename":"UDM","MSISDN":[8618663625533],"time_range_list":[{"start_time":1339295237,"end_time":1339295237}]}
	public JSONObject requestAccept(JSONObject jsonObject){
		
		if(jsonObject.containsKey("tablename")){
			String tablename = jsonObject.get("tablename").toString();
			String ip = jsonObject.getString("IP").toString();
			String msisdn = jsonObject.getString("MSISDN").toString();
			String sessionId = jsonObject.getString("session");
			JSONArray jsonArray = jsonObject.getJSONArray("time_range_list");
			ArrayList<Long> startlist = new ArrayList<Long>();
			ArrayList<Long> endlist = new ArrayList<Long>();
			for(int i=0;i<jsonArray.size();i++){
				JSONObject obj = jsonArray.getJSONObject(i);
				//开始时间
				String startTime = obj.get("start_time").toString();
				if(!startTime.equals("") && startTime != null){
					startlist.add(Long.parseLong(startTime));
				}
				String endTime = obj.get("end_time").toString();
				if(!endTime.equals("") && endTime != null){
					endlist.add(Long.parseLong(endTime));
				}
			}
			String sql = getSql(jsonObject);
			String hadoopPath = conf.get("hadoop.install.dir");
			String hdfsPath = conf.get("fs.default.name");
			//提交job代码
			for(int i=0;i<startlist.size();i++){
				Job job = createJob(hadoopPath+"CDR_ChinaMobile.jar","org.apache.hadoop.hdfs.job.framework.DoNNDIR",getHostIP.getLocalIP(),sessionId,hdfsPath,sql,new Text("CDR_Search"),startlist.get(i).toString(),endlist.get(i).toString(),ip);
			}
			//提取数据代码
			//多线程读取文件代码
			LOG.info("createJob ! ");
			return null;
		}else{
			LOG.info("JSONObject not find 'tablename' key, style is error !");
			return null;
		}
	}
	
	/*
	 * 解析json串并拼凑SQL语句
	 */
	public static String getSql(JSONObject obj) {
		String tableName = obj.getString("tablename").toLowerCase();
		String sql = "select all from " + tableName;
		sql += " on cdr where ";
		Set set = obj.entrySet();
		Iterator it = set.iterator();
		Boolean f = false;
		while (it.hasNext()) {
			Map.Entry<String, Object> my = (Map.Entry<String, Object>) it.next();
			String key = my.getKey().trim();
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
					if(obj.get("ISDownCDR")!=null){
						//下钻只匹配end_time_s参数
						sql += "END_TIME >= "+ obj1.getString("start_time") +" and END_TIME<=" + obj1.getString("end_time") + " or  ";
					}else {
						//CDR查询只匹配start_time_s参数
						sql += "START_TIME>=" + obj1.getString("start_time")	+ " and START_TIME<=" + obj1.getString("end_time") + " or  ";
					}
				}
				sql = sql.substring(0,sql.length() - 4);
				sql += ") and ";
				
			}else if(key.equalsIgnoreCase("mscid")){
				String value = my.getValue().toString();
				if(!value.equals("[]")){
					String mscid = value.substring(1,value.length()-1);
					if(mscid.contains(",")){
						sql += "mscid in ("+value.substring(1,value.length()-1)+") and ";
					}else{
						sql += "mscid == " + mscid + " and ";
					}
				}
			}else if(key.equalsIgnoreCase("mscidn")){
				String value = my.getValue().toString();
				if(!value.equals("[]")){
					String mscidn = value.substring(1,value.length()-1);
					if(mscidn.contains(",")){
						sql += "MSCIDN in ("+value.substring(1,value.length()-1)+") and ";
					}else{
						sql += "MSCIDN == " + mscidn + " and ";
					}
				}
			}else if (key.equalsIgnoreCase("pair_elem_list")) {
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
			}  else if(!key.equalsIgnoreCase("protocol") && !key.equalsIgnoreCase("where") && !key.equalsIgnoreCase("ISDownCDR")){
				String value = my.getValue().toString();
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
				} else {
					if(value.contains("?") || value.contains("*")){
						sql += " " +key + " like " + value + " and ";
					} else {
						sql += " " +key + "==" + value + " and ";
					}

				}
			} else if (key.equalsIgnoreCase("where")){
				String value = my.getValue().toString();
				sql += " " + value + " and ";
			}
			my = null;
		}
		sql = sql.substring(0,sql.length() -4);
		obj = null;
		set = null;
		it = null;
		return sql;
	}
	
	private  static Job createJob (String jar,String cclass,String localIP,String sessionId,String hdfsPath,String sql,Text jobName,String startTime,String endTime,String ip){		
		Configuration Myconf = new Configuration();
		Job job = new Job();
		LOG.info("====================yzy======================" + ip);
		job.getConf().set("cProc.dataio.ip",ip);
		job.getConf().set("hdfs.job.jar",jar);
		job.getConf().set("hdfs.job.class",cclass);
		job.getConf().set("hdfs.job.from.ip",localIP);
		job.setJobName(jobName);
		job.getConf().set("fs.default.name", hdfsPath);
		job.getConf().set("hdfs.job.param.sessionId",sessionId);
		job.getConf().set("cProc.starttime", startTime );
		job.getConf().set("cProc.endtime", endTime );
		job.getConf().set("timeout.times", timeout_times );
		int  indexNum = 0;
		if(sql.contains("calling_number") || (sql.contains("calling") && sql.contains("abis"))){
			indexNum = 0 ;//calling索引目录
		}
		if(sql.contains("called_number") && !sql.contains("calling_number") && !sql.contains("map") || (sql.contains("called") && sql.contains("abis") && !sql.contains("calling"))){
			indexNum = 1;//called索引目录
		}
		String indexNameString = "" ; 
		if(!sql.contains("called_number") && !sql.contains("calling_number") && !sql.contains("call_number") && !sql.contains("calling") && !sql.contains("called")){
			if(sql.contains("imsi")){
				indexNameString = "imsiIndex";//imsi索引目录
			}else if(sql.contains("imei")){
				indexNameString = "imeiIndex";//imei索引目录
			}else{
				if(sql.contains("begin_ts_s") || sql.contains("start_time_s")){
					indexNameString = "calledIndex";
				}else{
					indexNameString = "drillIndex";//下钻索引目录
				}	
			}
		}else{
			indexNameString = IndexChooser.getIndexName(indexNum);//true called = 1
		}
		if(sql.contains("abis")){
			if(sql.contains("calling_imei")){
				indexNameString = "callingImeiIndex";
			}else if (sql.contains("called_imei")){
				indexNameString = "calledImeiIndex";
			}else if (sql.contains("calling_imsi")){
				indexNameString = "callingImsiIndex";
			}else if (sql.contains("called_imsi")){
				indexNameString = "calledImsiIndex";
			}else if (sql.contains("calling")){
				indexNameString = "callingIndex";
			}else if (sql.contains("called")){
				indexNameString = "calledIndex";
			}else{
				if(sql.contains("begin_ts_s") || sql.contains("start_time_s")){
					indexNameString = "calledIndex";
				}else{
					indexNameString = "drillIndex";
				}				
			}
			sql = sql.replace("abis","tbl_abis_cdr");
		}
		if(sql.contains("udm")){
			indexNameString = "testIndex";
		}
		IndexChooser a = new IndexChooser(sql);
		String indexPath = TableAndIndexTool.getIndexFormat(a.getTableName(), indexNameString, a.getAppName());//索引文件
		String indexDataPath = TableAndIndexTool.getIndexDataPath(a.getTableName(), indexNameString, a.getAppName());//数据文件目录
		sql = a.getNewSql();
		Boolean sourceFilter = false;
		LOG.info("indexPath : " + indexPath);
		try {
			sourceFilter = a.isNeedFilterData(new ParseIndexFormat(indexPath,FileSystem.get(Myconf)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	//	LOG.info(" sourceFilter =================================================== "+sourceFilter);
		job.getConf().set("cProc.cdrSQL",sql);
		job.getConf().set("hdfs.job.param.indexDir",indexDataPath);
		job.getConf().set("cProc.sourceFilter",""+sourceFilter);
		job.getConf().set("cProc.hdfs",conf.get("fs.default.name"));	
		job.getConf().set("cProc.path",indexPath);
		if(sql.contains("abis")){
			job.getConf().set("cProc.table.name", "abis");
		}else{
			job.getConf().set("cProc.table.name", a.getTableName());
		}
		Myconf = null;
		a = null;
		sourceFilter = null;
		return job;
	}
}
