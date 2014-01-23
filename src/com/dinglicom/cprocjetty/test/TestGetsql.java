package com.dinglicom.cprocjetty.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import protocol.json.JsonRequest;

import com.cProc.CDR.action.JobServlet;
import com.cProc.CDR.util.ChooseIndexUtil;
import com.dinglicom.decode.util.CDRFieldUtil;

public class TestGetsql {
    
	 public TestGetsql(){
		 
	 }
	 
	 public void TestGetsql2(){
		 String sessionId1 = "aedcdf77-fa4c-4340-82c4-0dda236dded4";
	     String jsonStr ="{\"command\":\"GET\", \"session\":\""+ sessionId1 +"\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1331603329,\"end_time_s\":1331604736}],\"called_number\":\"10086\"}}";
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
		    //LOG.info("starttime is : " + startTime);
		    //LOG.info("endtime is : " + endTime);
		    String sessionId =jsonRequest.getSession();
		    String command =  jsonRequest.getCommand();
//		    String sql ="";
		    String condition = jsonRequest.getCondition().toString();
	     
		 getSql(condition, sessionId);
	 }
	 
		/**
		 * 查询表结构Map
		 */
	 private static final Map<String,List<String>> TABLE_MAP = new HashMap<String,List<String>>();
	 
		public static List<String> getFileds(String tablename){
			List<String> jastr = CDRFieldUtil.getCDRField(tablename);
			List<String> jaList = new ArrayList<String>();
			for(int i=0;i<jastr.size();i++){
				jaList.add(jastr.get(i));
			}
			return jaList;
		}
	 
	 public String getSql(String condition,String sessionId) {
			// 初始化TABLE_MAP,存储表结构   ,表结构在那个目录下
			TABLE_MAP.put("BSSAP",getFileds("BSSAP"));
			TABLE_MAP.put("BICC",getFileds("BICC"));
			TABLE_MAP.put("IUCS",getFileds("IUCS"));
			TABLE_MAP.put("MAP",getFileds("MAP"));
			TABLE_MAP.put("CAP",getFileds("CAP"));
			TABLE_MAP.put("ABIS",getFileds("abiscdr"));

			//databasename 是什么
			//String databaseName = conf.get("hadoop.database.name");
			String   databaseName = "cdr";
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
							if(obj.get("ISDownCDR")!=null){
								if(tableName.equalsIgnoreCase("ABIS")){
									//下钻只匹配end_time_s参数
									sql += "end_ts_s >= "+ obj1.getString("start_time_s") +" and end_ts_s<=" 
									+ obj1.getString("end_time_s") + " or  ";
								}else{
									//下钻只匹配end_time_s参数
									sql += "end_time_s >= "+ obj1.getString("start_time_s") +" and end_time_s<=" 
									+ obj1.getString("end_time_s") + " or  ";
								}
							}else {
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
//							if(value.contains("?")){
//								
//							}else if (value.contains("*")){
//								if(my.getKey().equals("calling_number")){
//									sql += " calling_number_str like '" + value.replace("*","%") + "' and ";
//								}else if(my.getKey().equals("called_number")){
//									sql += " called_number_str like '" + value.replace("*","%") + "' and ";
//								}else{
//									return null;
//								}
////								sql += " " +key + " like '" + value.replace("*","%") + "' and ";
//							} else {
//								if(my.getKey().equals("calling_number") && value.startsWith("0")){
//									sql += " calling_number_str=='" + value + "' and ";
//								}else if(my.getKey().equals("called_number") && value.startsWith("0")){
//									sql += " called_number_str=='" + value + "' and ";
//								}else{
//									sql += " " +key + "==" + value + " and ";
//								}
//							}
							
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
//								if(value.startsWith("?")){
//									if(value.length()>1){
//										sql += " "+key+" >= "+Integer.parseInt(value.replace("?","0"))+" and "+key+" <= 9" + value.substring(1).replace("?","9") + " and ";
//									}else{
//										sql += " "+key+" >= 0 and "+key+" <= 9 and ";
//									}
//								}else{
//									String replace = value;
//									sql += " "+key+" >= "+value.replace("?","0")+" and "+key+" <= " + replace.replace("?","9") + " and ";
//								}
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
				//ChooseIndexUtil chooseIndexUtil = new ChooseIndexUtil();
				if(objStr.equals("")){
					if(tableName.equals("abiscdr")){
						objStr = "begin_ts_s";
					}else{
						objStr = "start_time_s";
					}
				}
				String indexName = getIndexLoader(objStr.substring(0,objStr.length()-1),tableName);
				//LOG.info("indexName is " + indexName);
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
	 
		/**
		 * 根据条件选取索引
		 */
		public String getIndexLoader(String condition,String tableName){
			String conditions[] = condition.split(",");
			//根据tableName选取索引文件
			File file = new File("/dinglicom/cproc/jetty"+"/tableInfo/"+tableName+"/indexfile");
			//File file = new File("./tableInfo/"+tableName+"/indexfile/");
			HashMap<String,Integer> tableMap = new HashMap<String,Integer>();
			int zhongjian = 0;
			String indexName = "";
			if(file.exists()){
				File files[] = file.listFiles();//声明目录下所有的文件 files[];
	        	for(int i=0;i<files.length;i++){//遍历目录下所有的文件
	        		System.out.println(files[i].getPath());
	        		if(tableName.equals("abiscdr")){//没有见到abis的表 ？？？
	        			tableName = "abis";
	        		}
	        		//怎样去匹配
	        		int score = matchIndex(files[i].getPath(),tableName,conditions);
	        		System.out.println("score " + score);
	        		if(zhongjian<score){
	        			zhongjian = score;
	        			tableMap.put(files[i].getName(), zhongjian);
	        			indexName = files[i].getName();
	        		}
	        		System.out.println("indexName " + indexName);
	        	}
	        	return indexName;
			}else{
				return null;
			}
		}
		
		/**
		 * 读取文件，匹配数据
		 */
		
		private int matchIndex(String path,String tableName,String[] conditions){
			try {
				/*
				tableName:bssap
				indexName:callingIndex
				other:calling_number:LONG
				time:start_time_s:INT
				other:cdr_type:BYTE
				other:imsi:LONG
				*/
				/*
				  tableName:bssap
	              indexName:drillIndex
	              time:end_time_s:INT
	              other:cdr_type:BYTE
	              other:cdr_result:BYTE
	              other:imsi:LONG
	              other:city_code:SHORT
	              other:bsc_spc:INT
	              other:msc_spc:INT
				 */
				int score = 10;
				int sourceFilter = 0;
				File indexFile = new File(path+"/index.frm");
				if(indexFile.exists()){
					BufferedReader reader;
					reader = new BufferedReader(new FileReader(indexFile));
					String str = "";
					//第一次读取时判断文件表名是否符合要求
					if((str = reader.readLine())!=null){
						if(!str.split(":")[1].equalsIgnoreCase(tableName)){
							//LOG.info("matchIndex tableName error !");
							return 0;
						}
					}
					//第二次读取时判断索引文件名是否符合要求
					if((str = reader.readLine())!=null){
						if(!str.split(":")[1].equalsIgnoreCase(indexFile.getParentFile().getName())){
							//LOG.info("matchIndex indexName error !");
							return 0;
						}
					}
					int state = 10;
					while((str = reader.readLine())!=null){
						if(!str.trim().equals("")){
							for(int i =0;i<conditions.length;i++){
								if(conditions[i].equalsIgnoreCase(str.split(":")[1].trim())){
									score +=10 + state;
									break;
								}else{
									//判断是否过滤源文件
									sourceFilter = 1;							
								}
							}
						}
						state -- ;
					}
					return score - sourceFilter;
				}else{
					return 0 ;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}
	 
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TestGetsql tgs = new TestGetsql();
		tgs.TestGetsql2();
	}

}
