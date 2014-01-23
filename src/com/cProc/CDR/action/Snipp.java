package com.cProc.CDR.action;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntity;

public class Snipp {
	public static void main(String[] args) {
		if (args.length == 0) {
			return;
		}
		getJobStatus(args[0]);
	}
	
	public static boolean getJobStatus(String jobId) {
//		String jobId = "";
		Configuration conf = new Configuration();
//		LOG.info("@yzy jobId is : "+jobId);
		if(jobId == null || jobId.equals("")){
			return false;
		}
		String namenode = "172.16.4.101:8888";
		CProcFrameworkProtocol cProcFrameworkNode = null;
		InetSocketAddress cProcFrameworkNodeAddr = NetUtils.createSocketAddr(namenode);
		try {
			cProcFrameworkNode = (CProcFrameworkProtocol) RPC.getProxy(CProcFrameworkProtocol.class,CProcFrameworkProtocol.versionID, cProcFrameworkNodeAddr,conf, NetUtils.getSocketFactory(conf,CProcFrameworkProtocol.class));
			String type = cProcFrameworkNode.getJobEntityType(jobId);
//			LOG.info("@yzy jobId is : "+jobId+" , jobType is : ====================" + type );
//			RPC.stopProxy(cProcFrameworkNode);
			if (type .equals("SUCCESS")) {
				System.out.println("SUCCESS");
//				return true;
			}
			JobEntity jobEntity = cProcFrameworkNode.getJobEntity(jobId);
			System.out.println("yzy : "+jobEntity.toString());
			if(!jobId.equals(jobEntity.getJobId().toString())){
				return false;
			}else{
				ConcurrentHashMap<String, JobEntity> conhashmap = jobEntity.getSubJobEntity();
				String wancheng = "";
				String weiwancheng = "";
				String wancheng1 = "";
				String weiwancheng1="";
				String wancheng2="";
				String weiwancheng2="";
				int num=0;
				int nonum=0;
				int num1=0;
				int nonum1=0;
				int num2=0;
				int nonum2=0;
				int nums=0;
				int nonums=0;
				Set set = conhashmap.entrySet();
				Iterator it = set.iterator();
				while (it.hasNext()) {
					Map.Entry<String, JobEntity> my = (Map.Entry<String, JobEntity>) it.next();
					String key = my.getKey().trim();
					JobEntity value = my.getValue();
					String valueType = value.getJobEntityType().toString();
//					System.out.println("主节点分发任务："+key);
//					System.out.println("主节点分发状态"+valueType);
					if(valueType.equalsIgnoreCase("SUCCESS")){
						wancheng += key+",";
						num++;
						nums++;
					}else{
						weiwancheng += key+",";
						nonum++;
						nonums++;
					}
					
					//DOINDEX任务
					Set set1 = value.getSubJobEntity().entrySet();
					Iterator it1 = set1.iterator();
					while (it1.hasNext()) {
						Map.Entry<String, JobEntity> my1 = (Map.Entry<String, JobEntity>) it1.next();
						String key1 = my1.getKey().trim();
						JobEntity value1 = my1.getValue();
						String valueType1 = value1.getJobEntityType().toString();
//						System.out.println("DOINDEX节点分发任务："+key1);
//						System.out.println("DOINDEX节点分发状态"+valueType1);
						if(valueType1.equalsIgnoreCase("SUCCESS")){
							wancheng1 += key1+",";
							num1++;
							nums++;
						}else{
							weiwancheng1 += key1+",";
							nonum1++;
							nonums++;
						}
						
						//DOSEARCH
						Set set2 = value1.getSubJobEntity().entrySet();
						Iterator it2 = set2.iterator();
						while (it2.hasNext()) {
							Map.Entry<String, JobEntity> my2 = (Map.Entry<String, JobEntity>) it2.next();
							String key2 = my2.getKey().trim();
							JobEntity value2 = my2.getValue();
							String valueType2 = value2.getJobEntityType().toString();
//							System.out.println("DOSEARCH节点分发任务："+key2);
//							System.out.println("DOSEARCH节点分发状态"+valueType2);
							if(valueType2.equalsIgnoreCase("SUCCESS")){
								wancheng2 += key2+"("+key1+"),";
								num2++;
								nums++;
							}else{
								weiwancheng2 += key2+"("+key1+"),";
								nonum2++;
								nonums++;
							}
						}
					}
				}
				System.out.println("总计完成："+nums+" 个，未完成："+nonums+" 个。");
				System.out.println("其中DONNIR完成："+num+" 个，未完成："+nonum+" 个。");
				System.out.println("分别是（完成）："+wancheng+"，（未完成）: "+weiwancheng);
				System.out.println("其中DOINDEX完成："+num1+" 个，未完成："+nonum1+" 个。");
				System.out.println("分别是（完成）："+wancheng1+"，（未完成）: "+weiwancheng1);
				System.out.println("其中DOSEARCH完成："+num2+" 个，未完成："+nonum2+" 个。");
				System.out.println("分别是（完成）："+wancheng2+"，（未完成）: "+weiwancheng2);
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			return true;
		}
		
		return false;
	}
}
