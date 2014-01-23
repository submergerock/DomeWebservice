package com.cProc.CDR.action;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.cstor.cproc.cloudComputingFramework.CProcFrameworkProtocol;
import org.cstor.cproc.cloudComputingFramework.JobEntity.JobEntity;

import com.cProc.CDR.util.ChooseIndexUtil;
import com.dinglicom.decode.util.CDRFieldUtil;

public class Snippet {
	
	/**
	 * 查询表结构Map
	 */
	private static final Map<String,List<String>> TABLE_MAP = new HashMap<String,List<String>>();
	
	/**
	 * 表结构List
	 * @param args
	 * @throws IOException
	 */
	private static List<String> BSSAP_LIST = new ArrayList<String>();
	
	public static void main(String[] args) throws IOException {
		if(args.length==0){
			return;
		}
		getJobStatus(args[0]);
//		if(false){
//			System.out.println("111true");
//		}
//		File file = new File("D:\\1\\");
//		for(int i =0;i<file.listFiles().length;i++){
//			
//			
//			System.out.println(file.listFiles()[i].getName());
//			if(file.listFiles()[i].getName().split("_")[0].equals("1")){
//				System.out.println("true");
//			}else{
//				System.out.println("false");
//			}
//		}
//		boolean f = false;
//		if(!f){
//			System.out.println("1");
//		}else{
//			System.out.println("2");
//		}
//		System.out.println(JobServlet.class.getResource(""));
			
//			File file = new File("Q:\\1337808601-1337824017-tbl_abis_cdr-1-server2110.cdr");
//			FileInputStream input = new FileInputStream(file);
//			AbisCloudExpCdrCDR capCDR = new AbisCloudExpCdrCDR();
//			byte[] rawInfo = new byte[capCDR.getSize()];
//			List<String> jastr = CDRFieldUtil.getCDRField("ABIS_cloud_exp_cdr");
//			List<String> jaList = new ArrayList<String>();
//			for(int i=0;i<jastr.size();i++){
//				jaList.add(jastr.get(i));
//			}
//			while(input.read(rawInfo)!=-1){
//				if(capCDR.decode(rawInfo)){
//					
//					System.out.println(capCDR.toString().split(",").length);
//				}
//			}
//			System.out.println(jaList);
//			input.close();
		
			
		//String jsonStr = "{\"sort\":\"end_time_s\",\"netElem\":\"12\",\"proType\":\"CAP\",\"timeRank\":\"60\",\"timeRangeList\":[{\"nBeginTime\":1336838400,\"nEndTime\":1336842000},{\"nBeginTime\":1336842000,\"nEndTime\":1336845600},{\"nBeginTime\":1336845600,\"nEndTime\":1336849200},{\"nBeginTime\":1336849200,\"nEndTime\":1336852800},{\"nBeginTime\":1336852800,\"nEndTime\":1336856400},{\"nBeginTime\":1336856400,\"nEndTime\":1336860000},{\"nBeginTime\":1336860000,\"nEndTime\":1336863600},{\"nBeginTime\":1336863600,\"nEndTime\":1336867200},{\"nBeginTime\":1336867200,\"nEndTime\":1336870800},{\"nBeginTime\":1336870800,\"nEndTime\":1336874400},{\"nBeginTime\":1336874400,\"nEndTime\":1336878000},{\"nBeginTime\":1336878000,\"nEndTime\":1336881600},{\"nBeginTime\":1336881600,\"nEndTime\":1336885200},{\"nBeginTime\":1336885200,\"nEndTime\":1336888800},{\"nBeginTime\":1336888800,\"nEndTime\":1336892400},{\"nBeginTime\":1336892400,\"nEndTime\":1336896000},{\"nBeginTime\":1336896000,\"nEndTime\":1336899600},{\"nBeginTime\":1336899600,\"nEndTime\":1336903200},{\"nBeginTime\":1336903200,\"nEndTime\":1336906800},{\"nBeginTime\":1336906800,\"nEndTime\":1336910400},{\"nBeginTime\":1336910400,\"nEndTime\":1336914000},{\"nBeginTime\":1336914000,\"nEndTime\":1336917600},{\"nBeginTime\":1336917600,\"nEndTime\":1336921200},{\"nBeginTime\":1336921200,\"nEndTime\":1336924800}],\"netElemIdList\":[8613441215,8613740404]}";
//		String jsonStr = "{\"sort\":\"end_time_s\",\"netElem\":\"12\",\"proType\":\"CAP\",\"timeRank\":\"60\",\"timeRangeList\":[{\"nBeginTime\":1336917600,\"nEndTime\":1336921200}],\"netElemIdList\":[8613440208,8613741766]}";
//		net.sf.json.JSONObject joa = new net.sf.json.JSONObject();
//		joa = net.sf.json.JSONObject.fromObject(jsonStr);
//		
//		//协议类型1.2 or 1.4
//		String key = joa.get("netElem").toString();
//		//时间范围集合
//		net.sf.json.JSONArray array = joa.getJSONArray("timeRangeList");
//		ArrayList<Long> startlist = new ArrayList<Long>();
//		
//		for(int i=0;i<array.size();i++){
//			net.sf.json.JSONObject obj1 = array.getJSONObject(i);
//			//开始时间
//			String starttime = obj1.get("nBeginTime").toString();
//			if(!starttime.equals("") && starttime != null){
//				startlist.add(Long.parseLong(starttime));
//			}
//		}
////		//开始时间
////		String starttime = obj1.get("nBeginTime").toString();
////		//结束时间
////		String endtime = obj1.get("nEndTime").toString();
//		//分析粒度
//		String timeRank = joa.get("timeRank").toString();
//		//文件名范围
//		net.sf.json.JSONArray array1 = joa.getJSONArray("netElemIdList");
//		String filename = array1.get(0).toString()+"_"+array1.get(1);
//		//协议名称
//		String tableName = joa.get("proType").toString();
//		//排序字段名
//		String sortName = joa.get("sort").toString();
//		if(sortName.equals("")){
//			sortName = "rrbe_time";
//		}else{
//			sortName = "rrbe_time";
//		}
//		
//		//转换分析粒度
//		int time_Rank = 0 ;
//		if(!timeRank.equals("") && timeRank!=null){
//			time_Rank = Integer.parseInt(timeRank);
//		}
//		
//		
////		long start_time = 0;
////		long end_time = 0;
////		if(!starttime.equals("") && starttime != null){
////			start_time = Long.parseLong(starttime);
////		}
//		
////		if(!endtime.equals("") && endtime != null){
////			end_time = Long.parseLong(endtime);
////		}
//		
//		//判断协议类型
//		int cdrProtocol = 0;
//		if(tableName.equalsIgnoreCase("BSSAP")){
//			cdrProtocol =  CDRProtocol.BSSAP;
//		}else if (tableName.equalsIgnoreCase("CAP")){
//			cdrProtocol =  CDRProtocol.CAP;
//		}else if (tableName.equalsIgnoreCase("IUCS")){
//			cdrProtocol =  CDRProtocol.IUCS;
//		}
//		String dataStr = "[";
//		QueryService queryService = new QueryService();//(1336834800, 3, Protocol.CAP, "", "8613444202_8613749296","start_time_s");
//		for(int jlist=0;jlist<startlist.size();jlist++){
//			String jdata = queryService.getData(startlist.get(jlist), time_Rank/5, cdrProtocol, "/smp/data_root",filename,sortName,null,0);
//			if(jdata!=null && !jdata.equals("") && !jdata.equals("null")){
//				String jArray[] = jdata.split(";");
////				jdata = "[";
//				for(int i=0;i<jArray.length;i++){
//					if(jArray == null || jArray[i].equals("null")){
//						break;
//					}
//					jdata = "";
//					jdata += "[" + jArray[i].toString() + "],";
//				}
////				jdata = jdata.substring(0,jdata.length()-1) + "]";
//				System.out.println("jdata : " + jdata);
//			}
//			dataStr +=  jdata;
//		}
//		dataStr = dataStr.substring(0,dataStr.length()-1) + "]";
//		System.out.println(dataStr);
//		
//		String a = "abc";
//		System.out.println(a.getBytes().length);
//		System.out.println(a.getBytes().length);
//		
//		
//		String aaaaaa = "select * from abis where ";
//		aaaaaa = aaaaaa.replace("abis", "tbl_abis_info");
//		System.out.println(aaaaaa);
//		
//		List<String> jastr = CDRFieldUtil.getCDRField("ABIS_cloud_exp_cdr");
//		List<String> jaList = new ArrayList<String>();
//		for(int i=0;i<jastr.size();i++){
//			jaList.add(jastr.get(i));
//		}
//		System.out.println(jaList);
//		TABLE_MAP.put("BSSAP",getFileds("BSSAP"));
//		TABLE_MAP.put("BICC",getFileds("BICC"));
//		TABLE_MAP.put("IUCS",getFileds("IUCS"));
//		TABLE_MAP.put("MAP",getFileds("MAP"));
//		TABLE_MAP.put("CAP",getFileds("CAP"));
//		TABLE_MAP.put("ABIS",getFileds("abiscdr"));
////		TABLE_MAP.put("ABIS",getFileds("abismr"));
////		TABLE_MAP.put("ABIS",getFileds("abisho"));
////		TABLE_MAP.put("ABIS",getFileds("abis_cloud_exp_ho_cdr"));
//		BSSAP_LIST = getTableInfo("BsSAP");
////		String jsonStr = "{\"command\":\"GET\", \"session\":\"cdaf57e4-dde6-4efc-af6a-a7b13ef75dda\", \"condition\":{\"protocol\":\"ABIS\",\"time_range_list\":[{\"start_time_s\":1342062000,\"end_time_s\":1342062180}],\"type\":[1]}}";
//		String jsonStr ="{\"command\":\"GET\", \"session\":\"7caec1bc-b5a7-449f-912c-52602318ef7c\", \"condition\":{\"ISDownCDR\":\"1\",\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1343232000,\"end_time_s\":1343318400}],\"imsi\":\"18662728611\",\"cdr_type\":[1,2]}}";
////		String jsonStr ="{\"command\":\"GET\", \"session\":\"aedcdf77-fa4c-4340-82c4-0dda236dded4\", \"condition\":{\"protocol\":\"BSSAP\",\"time_range_list\":[{\"start_time_s\":1331602630,\"end_time_s\":1331604198}],\"calling_number\":\"10086\"}}";
////		String jsonStr ="{\"command\":\"GET\", \"session\":\"aedcdf77-fa4c-4340-82c4-0dda236dded4\", \"condition\":{\"result\":[2,1,3,4],\"protocol\":\"ABIS\",\"time_range_list\":[{\"start_time_s\":1337580994,\"end_time_s\":1337584594}],\"type\":[1,17,2],\"MSISDN\":\"13811443431\",\"begin_ci\":[1342815725,1342815726,1342815731,1342815732]}}";
//		
//		
//		
//		String aaa = "1360955241,1360955251,1360954911,1360954921,1360955261,1360961546,1360961556,1360955461,1360955471,1360955391,1360955481,1360955401,1360972037,1360972038,1360972039,1360954931,1360954941,1360954951,1360954961,1360954971,1360954981,1360955001,1360955021,1360955051,1360955061,1360955071,1360934296,1360934276,1360934286,1360972087,1360972088,1360972089,1360955271,1360928736,1360955081,1360955091,1360961896,1360961916,1360964426,1360964416,1360955101,1360955111,1360955121,1360972047,1360972048,1360972049,1360972053,1360972054,1360972055,1360934266,1360972063,1360972064,1360972065,1360955281,1360955301,1360955321,1360955331,1360955341,1360955361,1360955371,1360955131,1360955141,1360965731,1360972506,1360937597,1360937598,1360937599,1360937603,1360937604,1360937605,1360955151,1360955161,1360955171,1360955181,1360955191,1360955201,1360955211,1360955221,1360955231,1342603901,1342604191,1342604201,1342603921,1342604211,1342603931,1342603941,1342603961,1342604221,1342604231,1342604241,1342604251,1342604261,1342604271,1342604281,1342604291,1342604301,1342604311,1342603971,1342603981,1342603991,1342604001,1342621747,1342621748,1342621749,1342621753,1342621754,1342621755,1342604041,1342604051,1342604321,1342604331,1342604061,1342621687,1342621688,1342621689,1342621690,1342621693,1342621694,1342621695,1342621792,1342604071,1342604341,1342604351,1342604361,1342604371,1342604381,1342604391,1342604401,1342604411,1342621697,1342621698,1342621699,1342621703,1342621704,1342621705,1342604421,1342604431,1342621707,1342621708,1342621709,1342621713,1342621714,1342621715,1342604081,1342604091,1342604121,1342604131,1342621757,1342621758,1342621763,1342621764,1342621765,1342604141,1342604441,1342604151,1342621717,1342621718,1342621719,1342621723,1342621724,1342621725,1342604451,1342621727,1342621728,1342621729,1342621733,1342621734,1342621735,1342604161,1342604171,1342604461,1342588076,1342588106,1342586526,1342586546,1342621737,1342621738,1342621739,1342621743,1342621744,1342604181,1342604471,1342604481,1342604491,1342621767,1342621768,1342621769,1342621773,1342621774,1342621775,1342604501,1342604521,1342604531,1342604541,1361026193,1361026194,1361026195,1361026199,1361026200,1361026201,1361033569,1361026037,1361000001,1361034293,1361034294,1361034295,1361034300,1361034301,1361012032,1361012052,1361014822,1361014832,1361035793,1361035794,1361035795,1361035799,1361035800,1361035801,1361028472,1361017192,1361027557,1361011022,1361033072,1361033052,1361033592,1361033612,1361033602,1361017492,1361022173,1361022174,1361022175,1360996672,1360996692,1361048235,1361048240,1361039892,1361012042,1361042852,1361009062,1361028923,1361028924,1361028925,1361028929,1361028930,1361028931,1361036042,1361043907,1361043937,1361043887,1361043917,1361043927,1361043947,1360996022,1361021732,1361000499,1361000500,1361000501,1361032507,1343167907,1343167908,1343167909,1343188510,1343183801,1343183808,1343186215,1343186175,1343186225,1343210475,1343210435,1343210455,1343210425,1343200745,1343210062,1343210063,1343210067,1343210068,1343210069,1343171919,1343206895,1343215185,1343206905,1343206925,1343206915,1343203820,1343209460,1343209310,1343214310,1343175010,1343187001,1343187002,1343187003,1343187007,1343187008,1343187009,1343221231,1343221232,1343221233,1343199065,1343208060,1343207640,1343207681,1343207682,1343207683,1343207687,1343207688,1343207689,1343215225,1343215235,1343215175,1343215165,1343215285,1343215295,1343206935,1343215195,1343215255,1343215245,1343144485,1343144486,1343144487,1343144491,1343144492,1343144493,1343137665,1343137666,1343137667,1343137671,1343137672,1343137673,1343136439,1343107344,1343107334,1343149459,1343151654,1343102694,1343102684,1343107145,1343107146,1343107147,1343107149,1343107150,1343107151,1343107152,1343107153,1343134779,1343156035,1343156036,1343156037,1343156041,1343156042,1343156043,1343149469,1343107424,1343158444,1343142309,1343142289,1343142299,1343142279,1343115904,1343148039,1343107494,1343099944,1343157734,1343115174,1343135945,1343135946,1343135947,1343135951,1343135952,1343135953,1343143504,1343135289,1343135299,1343135279,1343146849,1343135309,1343117964,1343138714,1343127934,1343107454,1343102554,1343102564,1343102574,1343117495,1343117496,1343117497,1343117501,1343117502,1343117503,1343144109,1343109615,1343109617,1343109623,1343104144,1343139849,1343139859,1343139869,1343139449,1343125854,1343125844,1343125824,1343129984,1343137685,1343137686,1343137687,1343137691,1343137692,1343137693,1343115294,1343138429,1343131874,1343139469,1343139479,1343139489,1343139499,1343139519,1343139529,1343139379,1343139409,1343139389,1343139419,1343139399,1343139429,1343142249,1343142259,1343139839,1343139829,1343139879,1343139889,1343139899,1343139909,1343149259,1343149269,1343149289,1343149299,1343149239,1343108324,1343127284,1343127294,1343127304,1343099714,1343104494,1343120265,1343120266,1343112934,1343149819,1343149779,1343149799,1343149809,1343149769,1343149789,1343110084,1343134155,1343134156,1343134157,1343134161,1343134162,1343134163,1343106889,1351916873,1351916863,1351916853,1351887083,1351899079,1351899080,1351899081,1351899085,1351899086,1351899087,1351921708,1351919128,1351913008,1351903829,1351903830,1351903831,1351903835,1351903836,1351903837,1351931303,1351934473,1351934483,1351906308,1351906068,1351884058,1351927808,1351884048,1351921573,1351895348,1351916938,1351916948,1351908988,1351908978,1351923428,1351907198,1351907168,1351930133,1351913928,1351926308,1351926348,1351940488,1351894818,1351924365,1351924366,1351924367,1351909808,1351882118,1351882108,1351917023,1351909698,1351924658,1351933378,1351920828,1360368542,1360353779,1360353781,1360385062,1360373292,1360358243,1360358244,1360358245,1360358249,1360358250,1360358251,1360348152,1360348162,1360348182,1360348142,1360332582,1360345472,1360343004,1360343005,1360343010,1360343011,1360343272,1360343262,1360366357,1360386410,1360348592,1360368957,1360341402,1360341412,1360374112,1360364723,1360364724,1360364729,1360364730,1360390982,1360382842,1360347752,1360372967,1360963903,1360963904,1360963905,1360969001,1360965611,1360969556,1360968241,1360937623,1360966081,1360983396,1360922306,1360922396,1360967826,1360980231,1360975301,1360974556,1360931606,1360960101,1360960111,1360974546,1360931703,1360923036,1360968053,1360968054,1360969833,1360969834,1360969835,1360979861,1360932021,1360935166,1360981373,1360981374,1360321096,1360281612,1360281613,1360281614,1360269166,1360312281,1360312261,1360312271,1360320867,1360279016,1360320880,1360320873,1360320875,1360279063,1360268227,1360268228,1360268229,1360268233,1360274756,1360312429,1360286386,1360323646,1360323676,1360302486,1360274026,1360274016,1360294526,1351870022,1351825372,1351825382,1351825392,1351825402,1351870672,1351832522,1351831593,1351831594,1351831599,1351831600,1351848202,1351872312,1351848242,1351822120,1351821942,1351855042,1351851607,1351816982,1351816972,1351816922,1351816932,1351857182,1351872862,1351822212,1351822152,1351858295,1351832412,1351873312,1351826109,1351826110,1351826111,1351857243,1351857244,1351857245,1351857249,1351857250,1351857251,1351832313,1351832316,1351832315,1351832321,1351822657,1351822667,1351850177,1351821950,1351821951,1351818342,1351859082,1351856941,1351859112,1351856247,1351827963,1351827964,1351827965,1351821959,1351821960,1351821961,1351864162,1351863622,1351859477,1351864232,1351833682,1351842824,1351842825,1351842831,1351812312,1351859117,1351862302,1351859137,1351859127,1351859157,1351859147,1351821692,1351821693,1351846907,1351851267,1351851827,1351858233,1351858234,1351858235,1351858239,1351858240,1351858241,1351821921,1351857377,1351857457,1351836462,1351836522,1351852702,1351846897,1351838849,1351838851,1351846757,1351846767,1342635309,1342635304,1342635305,1342635306,1342635307,1342635308,1342635310,1342635312,1342635315,1342635311,1342635302,1342635303,1342635314,1342633376,1342611976,1342591066,1342607446,1342579146,1342579156,1342579166,1342592371,1342592381,1342584757,1342622411,1342622421,1342584758,1342584759,1342629097,1342629098,1342629099,1342596046,1342629103,1342629104,1342601916,1342606176,1342606156,1342606166,1342592361,1342592351,1342592341,1342592331,1342578766,1342615696,1342615736,1342595996,1342599116,1342590567,1342590568,1342590569,1342590574,1342590575,1342600466,1342614241,1342610181,1342610191,1342610211,1342610221,1342622597,1342622598,1342622599,1342622600,1342622603,1342598017,1342598018,1342598019,1342622767,1342622768,1342622774,1342622775,1342597407,1342597408,1342597409,1342597416,1342597413,1342597414,1342597415,1342622770,1342622557,1342622558,1342622559,1342572563,1342612531,1342612541,1342612521,1342631646,1342625626,1342596031,1342612246,1342622547,1342622548,1342622549,1342603987,1342603988,1342603989,1342603993,1342603994,1342603995,1342620033,1342620034,1342620035,1342582166,1342624681,1342606266,1342625457,1342625458,1342625459,1342625463,1342625464,1342625465,1342612921,1342612931,1342614541,1342575596,1342593726,1342618081,1342602726,1342616396,1342615636,1342614726,1342614696,1342592886,1342603936,1342610411,1342606697,1342606698,1342627316,1342585266,1342593956,1342617939,1342621163,1342621164,1342593656,1342620306,1342593557,1342593558,1342593559,1342593563,1342593564,1342593565,1342579926,1342579886,1342579906,1342579896,1342579936,1342616681,1342589503,1342589504,1342589505,1342632666,1342629316,1342629306,1342620307,1342620308,1342620309,1342620313,1342620314,1342620315,1342625854,1342625852,1342625853,1342625849,1342625855,1342625857,1342625850,1342625858,1342595371,1342585596,1342599131,1342601031,1342619551,1360526616,1360526617,1360471145,1360471146,1360471147,1360471151,1360471152,1360471153,1360507219,1360507229,1360507779,1360507799,1360507819,1360488444,1360524844,1360499044,1360483839,1360464444,1360483109,1360483099,1360483493,1360483651,1360499699,1360499689,1360464464,1360495584,1360511214,1360478854,1360509161,1360509162,1360509163,1360522279,1360511534,1360481144,1360481194,1360522269,1360481804,1360481104,1360507769,1360507789,1360507809,1360522314,1360522304,1360464064,1360509109,1360509119,1351621974,1351671119,1351661686,1351675658,1351675661,1351675662,1351675663,1351652654,1351652674,1351621854,1351646814,1351649614,1351675395,1351675396,1351675397,1351652774,1351655124,1351655144,1351663264,1351630979,1351664814,1351661654,1351676821,1351676822,1351676823,1351656591,1351656592,1351656593,1351661724,1351676951,1351676953,1351657124,1351677154,1351677164,1351662174,1351656464,1351661599,1351661589,1351653629,1351670386,1351628799,1351645069,1351635809,1351659669,1351667519,1351655549,1351661889,1351618285,1351618286,1351618287,1351618291,1351618293,1351625711,1351625712,1351625713,1351623844,1351666444,1351671388,1351669164,1351664924,1351669299,1351669289,1351649459,1351645494,1351677184,1351669089,1351664804,1351667919,1351656219,1351655149,1351628189,1351667044,1351646014,1351625314,1351619134,1351621724,1351626755,1351626756,1351626757,1351626761,1351626762,1351626763,1351625934,1351625964,1351625984,1351625974,1351625944,1351653859,1351665674,1351663084,1351663374,1351665449,1351665439,1351677074,1351677124,1351631084,1351636189,1351645049,1351645059,1342963014,1342948611,1342948621,1342944636,1342944426,1342944551,1342921006,1342947867,1342947868,1342947869,1342947873,1342947874,1342947875,1342957936,1342910553,1342961216,1342934996,1342916517,1342916518,1342916519,1342958271,1342920553,1342920554,1342920555,1342905606,1342930976,1342930626,1342931076,1342930606,1342930726,1342930616,1342930776,1342931066,1342930966,1342925816,1342925796,1342945956,1342945966,1342949347,1342949348,1342949349,1342934391,1342961557,1342961558,1342916306,1342961636,1342951551,1342929276,1342916436,1342916336,1342945481,1342945721,1342945381,1342939466,1342962016,1342962026,1342962046,1342900616,1342941961,1342941981,1342942011,1342942021,1342941971,1342952847,1342952848,1342952849,1342952853,1342952854,1342952855,1342947383,1342947384,1342947385,1342943746,1342943756,1342943736,1342916126,1342944371,1342928246,1342934276,1342949166,1342949176,1342949186,1342916697,1342916698,1342916699,1342916702,1342916704,1342916705,1342916701,1342927176,1342939363,1342939364,1342939365,1342920537,1342920538,1342920539,1342920543,1342920544,1342920545,1342925116,1342922563,1342922564,1342922565,1342961186,1342918417,1342918418,1342918419,1342918423,1342918424,1342918425,1342935256,1342927756,1373288939,1373257699,1373257659,1373257669,1373288589,1373288599,1373251379,1373251399,1373251409,1373251429,1373245094,1373284669,1373268601,1373268602,1373285145,1373285146,1373285147,1373285151,1373285152,1373285153,1373290065,1373290066,1373290067,1373290071,1373290072,1373290073,1373290364,1373290384,1373257489,1373257499,1373257509,1373257549,1373257719,1373257639,1373245211,1373245212,1373245213,1373262504,1373262494,1373265364,1373282984,1373282994,1373276994,1373264604,1373265019,1373283249,1373283239,1373289034,1373265394,1373264974,1373265104,1373265124,1373265444,1373267405,1373267406,1373267407,1373267411,1373267412,1373267413,1373291175,1373291176,1373291177,1373291181,1373291182,1373291183,1373256079,1373256089,1373256099,1373256109,1373256119,1373284039,1373284029,1373284019,1373284009,1373284059,1373284049,1373284079,1373284069,1373256049,1373256059,1373256069,1373284089,1373284099,1373299269,1373299249,1373299259,1373251574,1373251604,1373251704,1373251449,1373251479,1373287629,1373287639,1373287659,1373287689,1373287679,1373287669,1373267204,1373267194,1373267224,1373266854,1373251359,1373295954,1373243504,1373243494,1373243514,1373243524,1373294029,1373294039,1373294049,1373291324,1373244224,1373244244,1373244344,1373272234,1373246304,1373246294,1373295454,1373274264,1373274294,1373274274,1373274284,1373282269,1373279679,1373289079,1373289069,1373283814,1373259169,1373268854,1373268864,1373268725,1373268726,1373268731,1373268732,1373288111,1373288112,1373247084,1373258364,1373258354,1373258254,1373258244,1373258264,1373258184,1373258204,1373258224,1373258234,1373289594,1373269474,1373295685,1373295686,1373295687,1373295688,1373248874,1373251955,1373251956,1373251957,1373251961,1373251962,1373251963,1373295254,1373283234,1373283244,1373283254,1373283264,1373277274,1373254564,1373284014,1373284054,1373284024,1373265744,1373249624,1373249634,1373259199,1373625724,1373589714,1373589724,1373578344,1373625534,1373573094,1373606465,1373606466,1373606467,1373580873,1373588429,1373592039,1373592624,1373585409,1373605404,1373605544,1373617275,1373617276,1373617277,1373607664,1373583864,1373625185,1373625186,1373625187,1373625191,1373625192,1373625193,1373584844,1373583504,1373610614,1373583574,1373583594,1373588559,1373588489,1373588419,1373627554,1373594164,1373613964,1373613974,1373613984,1373613994,1373586899,1373612079,1373592834,1373592724,1373618154,1373589404,1373625524,1373594674,1360817714,1360817704,1360807935,1360807936,1360807937,1360807941,1360807942,1360807943,1360816684,1360837824,1360805015,1360805016,1360805017,1360802404,1360852794,1360837674,1360837059,1360837069,1360838399,1360842874,1360842864,1360818099,1360807684,1360830284,1360830294,1360815084,1360811274,1360825424,1360839709,1360841584,1360825324,1360839719,1360824444,1360795124,1360842734,1360836061,1360836062,1360805045,1360805046,1360805047,1360805051,1360805052,1360805053,1360816655,1360816656,1360816657,1360816661,1360816662,1360816663,1360814554,1360814544,1360804764,1360828969,1360801825,1360801826,1360801827,1360801831,1360801832,1360801833,1360837079,1360801455,1360801456,1360801457,1360840634,1360843965,1360843966,1360843967,1360843969,1360843971,1360843972,1360843973,1360824674,1360805164,1360805174,1360798394,1360798324,1360794554,1360794564,1360801555,1360801556,1360801557,1360801561,1360801562,1360801563,1360802224,1360805574,1360805704,1360815239,1360815229,1360805075,1360805076,1360805077,1360805081,1360805082,1360805083,1360811524,1360802354,1360814274,1360803574,1360842709,1360842699,1360842719,1360837404,1360837014,1360803825,1360803865,1360803855,1360803835,1360804555,1360804556,1360804557,1360804561,1360837774,1360837784,1360837804,1360840429,1360842809,1360842789,1360842799,1360842779,1360842769,1360830321,1360830322,1360830323,1360836579,1360804144,1360815874,1360815884,1360804075,1360804076,1360804077,1360804081,1360836669,1360836699,1360838519,1360836659,1360833189,1360833199,1360804894,1360805184,1360846470,1360805085,1360805086,1360805087,1360805091,1360805092,1360805093,1360816219,1360837394,1360837384,1360812564,1360833314,1360842819,1360804525,1360804526,1360804527,1360804531,1360804532,1360804533,1360804578,1360852594,1360819534,1360819544,1360819524,1360836051,1360844065,1360803846,1360838084,1360837794,1360829089,1360821894,1360821904,1360806559,1360806589,1360806549,1360806459,1360806519,1360806399,1360806439,1360806419,1360806389,1360795254,1360833144,1360833154,1360833134";
//		System.out.println("aaa : "+aaa.split(",").length);
//		
//		String jsonStr ="{\"command\":\"GET\", \"session\":\"aedcdf77-fa4c-4340-82c4-0dda236dded4\", \"condition\":" +
//		"{\"protocol\":\"bssap\",\"time_range_list\":[{\"start_time_s\":1337580994,\"end_time_s\":1337584594}],\"cdr_type\":[1,17,2],\"Where\":\"calling_number == 10086 or called_number == 10086\"}}";
//		net.sf.json.JSONObject jsonObject = new net.sf.json.JSONObject();
//		jsonObject = net.sf.json.JSONObject.fromObject(jsonStr);
//		JsonRequest jsonRequest = new JsonRequest();
//		jsonRequest.unpack(jsonStr);
//		JSONObject jsonObject1 = (JSONObject)JSONValue.parse(jsonRequest.getBody());
//		jsonObject1 = (JSONObject) jsonObject1.get("condition");
////		System.out.println(jsonObject1.toString());
//		System.out.println(getSql(jsonObject1.toString(),"7caec1bc-b5a7-449f-912c-52602318ef7c"));
//		
		
//		String sqlString = "select all from bssap on cdr where (start_time_s>=1387846800 and start_time_s<=1387846860 ) and  cdr_type in (1,2) and  (calling_number == 10086 or called_number == 10086 )";
//		IndexChooser a = new IndexChooser(getSql(jsonRequest.getCondition().toString(), "aedcdf77-fa4c-4340-82c4-0dda236dded4"));
//		System.out.println(a.getNewSql());
//		
//		WriteSQL("aaaaaa" + System.currentTimeMillis());
		
		
		
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
//		finally{
//			RPC.stopProxy(cProcFrameworkNode);
//		}
		
		return false;
	}
	
	public static void WriteSQL(String sql) throws IOException{
		File file = new File("d://sql.txt");
		if(!file.exists()){
			file.createNewFile();
		}
		BufferedWriter output = new BufferedWriter(new FileWriter(file,true));
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
		String out = df.format(new Date()) + "," + sql;// new Date()为获取当前系统时间
		output.write(out);
		output.write("\n");
		output.close();
	}
	
	public static List<String> getFileds(String tablename){
		List<String> jastr = CDRFieldUtil.getCDRField(tablename);
		List<String> jaList = new ArrayList<String>();
		for(int i=0;i<jastr.size();i++){
			jaList.add(jastr.get(i));
		}
		System.out.println(jaList);
		return jaList;
	}
	
	public static String getSql(String condition,String sessionId) {
		String databaseName = "cdr";
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
		String objStr = "";
		List<String> list = TABLE_MAP.get(tableName.toUpperCase());
		while (it.hasNext()) {
			Map.Entry<String, Object> my = (Map.Entry<String, Object>) it.next();
			String key = my.getKey().trim();
			if(list.contains(key.toLowerCase()) || key.equals("protocol") || key.equals("time_range_list") || key.equals("Where") || key.equals("ISDownCDR") ||  key.equals("pair_elem_list")){ 
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
								sql += "end_ts_s >= "+ obj1.getString("start_time_s") +" and end_ts_s<=" + obj1.getString("end_time_s") + " or  ";
							}else{
								//下钻只匹配end_time_s参数
								sql += "end_time_s >= "+ obj1.getString("start_time_s") +" and end_time_s<=" + obj1.getString("end_time_s") + " or  ";
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
				}else if(!key.equalsIgnoreCase("protocol") && !key.equalsIgnoreCase("where") && !key.equalsIgnoreCase("ISDownCDR")){
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
			System.out.println("indexName is " + indexName);
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
	
	public static String getSql(String condition) {
		String databaseName = "cdr";
		net.sf.json.JSONObject obj = net.sf.json.JSONObject.fromObject(condition);
		String tableName = obj.getString("protocol").toLowerCase();
		String sql = "select all from " + tableName;
		sql += " on " + databaseName + " where ";
		Set set = obj.entrySet();
		Iterator it = set.iterator();
		List<String> list = TABLE_MAP.get(tableName.toUpperCase());
		while (it.hasNext()) {
			Map.Entry<String, Object> my = (Map.Entry<String, Object>) it.next();
			String key = my.getKey().trim();
			if(list.contains(key.toLowerCase()) || key.equals("protocol") || key.equals("time_range_list") || key.equals("Where") || key.equals("ISDownCDR")){
			if (key.equalsIgnoreCase("time_range_list")) {
				String value = my.getValue().toString();
				net.sf.json.JSONArray array =net.sf.json.JSONArray.fromObject(value);
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
						sql += "end_time_s >= "+ obj1.getString("start_time_s") +" and end_time_s<=" + obj1.getString("end_time_s") + " or  ";
					}else {
						//CDR查询只匹配start_time_s参数
						sql += "start_time_s>=" + obj1.getString("start_time_s")	+ " and start_time_s<=" + obj1.getString("end_time_s") + " or  ";
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
					if(value.contains("*") && !value.contains("?")){
						if(my.getKey().equals("calling_number")){
							sql += " calling_number_str like '" + value.replace("*","%") + "' and ";
						}else if(my.getKey().equals("called_number")){
							sql += " called_number_str like '" + value.replace("*","%") + "' and ";
						}else{
							return null;
						}
					}else if (value.contains("?") && !value.contains("*")){
						if(value.startsWith("?")){
							if(value.length()>1){
								sql += " "+key+" >= "+Integer.parseInt(value.replace("?","0"))+" and "+key+" <= 9" + value.substring(1).replace("?","9") + " and ";
							}else{
								sql += " "+key+" >= 0 and "+key+" <= 9 and ";
							}
						}else{
							String replace = value;
							sql += " "+key+" >= "+value.replace("?","0")+" and "+key+" <= " + replace.replace("?","9") + " and ";
						}
					} else {
						return null;
					}

				}
			} else if (key.equalsIgnoreCase("where")){
				String value = my.getValue().toString();
				sql += " " + value + " and ";
			}
			my = null;
			}else{
				return null;
			}
		}
		sql = sql.substring(0,sql.length() -4);
		databaseName = null;
		obj = null;
		set = null;
		it = null;
		String indexNameString = "";
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
			sql = sql.replace("abis","abiscdr");
		}
		if(sql.contains("udm")){
			indexNameString = "testIndex";
		}
		System.out.println(indexNameString);
		return sql;

	}
	
	/*
	 * 扫描tableInfo信息
	 */
	private static List<String> getTableInfo (String tableName) throws IOException{
		List<String> tableList = new ArrayList<String>();
//		String url = Snippet.class.getClassLoader().toString()+"";
		File file = new File("C:/Users/yunchuang/Desktop/tableInfo/"+tableName.toLowerCase()+"/table.frm");
		if(file.exists()){
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String str = "";
			//第一读取时判断文件表名是否符合要求
			if((str = reader.readLine())!=null){
				if(!str.split(":")[1].equalsIgnoreCase(tableName)){
					System.out.println("getTableInfo tableName error !");
					return null;
				}
			}
			while((str = reader.readLine())!=null){
				if(!str.trim().equals("")){
//					System.out.println(str.split(",")[0].split(":")[1].trim());
					tableList.add(str.split(",")[0].split(":")[1].trim());
				}
			}
			return tableList;
		}else{
			return null;
		}
	}
	
	/*
	 * 请求接口 
	 * param: JSONObject {"tablename":"perf_o_tdr_v1","IP":"172.23.27.74","msisdn":"8618663306073","time_range_list":[{"start_time_s":1337580994,"end_time_s":1337584594}]]}
	 * return String[]
	 */
	public String[] requestAccept(net.sf.json.JSONObject jsonObject){
		if(jsonObject.containsKey("tablename")){
			String tablename = jsonObject.get("tablename").toString();
			String ip = jsonObject.getString("IP").toString();
			String msisdn = jsonObject.getString("msisdn").toString();
			JSONArray jsonArray = jsonObject.getJSONArray("time_range_list");
			ArrayList<Long> startlist = new ArrayList<Long>();
			ArrayList<Long> endlist = new ArrayList<Long>();
			for(int i=0;i<jsonArray.size();i++){
				net.sf.json.JSONObject obj = jsonArray.getJSONObject(i);
				//开始时间
				String startTime = obj.get("start_time_s").toString();
				if(!startTime.equals("") && startTime != null){
					startlist.add(Long.parseLong(startTime));
				}
				String endTime = obj.get("end_time_s").toString();
				if(!endTime.equals("") && endTime != null){
					endlist.add(Long.parseLong(endTime));
				}
			}
		}else{
			System.out.println("JSONObject not find 'tablename' key, style is error !");
			return null;
		}
		 
		return null;
	}
}

