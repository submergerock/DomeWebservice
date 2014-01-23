package com.cProc.RPC.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Test {

	public static class CDRBeanForIndex {
		
		public String calling_number;//主叫
		public String called_number;//被叫
		public String cdr_type;//cdr类型
		public long[] start_time_s;//开始时间，
		public long[] end_time_s;//结束时间
		public String opc_dpc;//局向设置
		public int netElem;//网元类型
		public String netElemId;//网元ID
		public String tableName="";//表名
		public int tableType=0;//表类型
		public int OnceCount;//要查的记录数
		public int BeginNum;//从哪条记录数开始查
		public String companyId;//商家
		
		
		//定义各个字段类型，一定要对应
		public int CallType;
		public int CDRCallType;//呼叫类型
		public Long CallTypeNum;//主、被
		public String Info;//用户信息
		public JSONArray ProtocolType;//协议类别
		public JSONArray CdrType;//业务类别
		public String caq;
		public JSONArray netElemIdList;//网元ID
		public JSONArray tacElemList;
		public JSONArray pairElemList;//局向设置
		public JSONArray pairIpList;
		public JSONArray timeRangeList;
		public int timeRank;//枚举？？
		public int staticDim;//枚举？？
		public JSONArray IMEIList;
		public String selectedKPI;
		public JSONArray kpiList;
		public JSONArray m_Provinces;
		public JSONArray m_Cities;
		public JSONArray m_MSCs;
		public JSONArray surSpcList;
		public JSONArray destSpcList;
		public int m_sumType;
		public String ShowType;
		public String OtherFilter;
		public boolean ISGSM;
		public boolean ISTD;
		public JSONObject timeRange;
		
		public CDRBeanForIndex(JSONObject object) {
			//接收到的json对象进行赋值
//			this.CallType = object.getInt("CallType");
			this.CDRCallType = object.getInt("CDRCallType");
			String CallTypeNumTemp = object.getString("CallTypeNum");
			if(CallTypeNumTemp != null && !CallTypeNumTemp.equals("null") && !CallTypeNumTemp.equals("")){
				this.CallTypeNum = Long.parseLong(CallTypeNumTemp);
			}                                                      
			this.CdrType = object.getJSONArray("CdrType");
			this.Info = object.getString("Info");
			this.ProtocolType= object.getJSONArray("ProtocolType");
			String tableNameTemp = object.getJSONArray("ProtocolType").toString();
			if(tableNameTemp!=null &&!"".equals(tableNameTemp))
				this.tableName = tableNameTemp.replace("[", "").replace("]", "");
			this.caq = object.getString("caq");
			
			this.netElem = object.getInt("netElem");
			
			this.netElemIdList = object.getJSONArray("netElemIdList");
			this.tacElemList = object.getJSONArray("tacElemList");
			this.pairElemList = object.getJSONArray("pairElemList");
			this.pairIpList = object.getJSONArray("pairIpList");
			this.timeRangeList = object.getJSONArray("timeRangeList");
			this.timeRank = object.getInt("timeRank");
			this.staticDim = object.getInt("staticDim");
			this.IMEIList = object.getJSONArray("IMEIList");
			this.selectedKPI = object.getString("selectedKPI");
			
			
			 
			this.kpiList = object.getJSONArray("kpiList");
			this.m_Provinces = object.getJSONArray("m_Provinces");
			this.m_Cities = object.getJSONArray("m_Cities");
			this.m_MSCs = object.getJSONArray("m_MSCs");
			this.CallType = object.getInt("CallType");
			this.surSpcList = object.getJSONArray("surSpcList");
			this.destSpcList = object.getJSONArray("destSpcList");
			this.m_sumType = object.getInt("m_sumType");
			this.ShowType = object.getString("ShowType");
			this.OtherFilter = object.getString("OtherFilter");
			this.ISGSM = object.getBoolean("ISGSM");
			this.ISTD = object.getBoolean("ISTD");
			this.timeRange = object.getJSONObject("timeRange");
			
			this.OnceCount = object.getInt("OnceCount");
			this.BeginNum = object.getInt("BeginNum");
		}
		
		public long[] getStart_time_s() {
			if (timeRangeList != null && timeRangeList.size() > 0) {
				start_time_s = new long[timeRangeList.size()];
				for (int j = 0; j < timeRangeList.size(); j++) {
					JSONObject object = timeRangeList.getJSONObject(j);
					start_time_s[j]=object.getLong("nBeginTime");
				}
			} else if (timeRange != null && timeRange.size() > 0) {
				start_time_s = new long[timeRange.size()];
				start_time_s[0]=timeRange.getLong("nBeginTime");
			}
			return start_time_s;
		}

		public long[] getEnd_time_s() {
			if (timeRangeList != null && timeRangeList.size() > 0) {
				end_time_s = new long[timeRangeList.size()];
				for (int j = 0; j < timeRangeList.size(); j++) {
					JSONObject object = timeRangeList.getJSONObject(j);
					end_time_s[j]=object.getLong("nEndTime");
				}
			} else if (timeRange != null && timeRange.size() > 0) {
				end_time_s = new long[timeRange.size()];
				end_time_s[0]=timeRange.getLong("nEndTime");
			}
			return end_time_s;
		}

		public void setEnd_time_s(long[] end_time_s) {
			this.end_time_s = end_time_s;
		}

		public void setStart_time_s(long[] start_time_s) {
			this.start_time_s = start_time_s;
		}

		public String getCdr_type() {
			//业务类别
			if(CdrType!=null && CdrType.size()>0){
				cdr_type = CdrType.toString().replace("[", "").replace("]", "");
			}
			return cdr_type;
		}

		public void setCdr_type(String cdr_type) {
			this.cdr_type = cdr_type;
		}

		public String getCalling_number() {
			//主被叫
			if(CDRCallType==0){
				calling_number=CallTypeNum+"";
			}else if(CDRCallType==1){
			}else if(CDRCallType==2){
				calling_number=CallTypeNum+"";
			}
			return calling_number;
		}

		public void setCalling_number(String calling_number) {
			this.calling_number = calling_number;
		}

		public String getCalled_number() {
			//主被叫
			if(CDRCallType==0){
			}else if(CDRCallType==1){
				called_number=CallTypeNum+"";
			}else if(CDRCallType==2){
				called_number=CallTypeNum+"";
			}
			return called_number;
		}

		public void setCalled_number(String called_number) {
			this.called_number = called_number;
		}

		public int getOnceCount() {
			return OnceCount;
		}
		public void setOnceCount(int onceCount) {
			OnceCount = onceCount;
		}
		public int getBeginNum() {
			return BeginNum;
		}
		public void setBeginNum(int beginNum) {
			BeginNum = beginNum;
		}
		public int getCallType() {
			return CallType;
		}
		public void setCallType(int callType) {
			CallType = callType;
		}
		public int getCDRCallType() {
			return CDRCallType;
		}
		public void setCDRCallType(int callType) {
			CDRCallType = callType;
		}
		public Long getCallTypeNum() {
			return CallTypeNum;
		}
		public void setCallTypeNum(Long callTypeNum) {
			CallTypeNum = callTypeNum;
		}
		public String getInfo() {
			return Info;
		}
		public void setInfo(String info) {
			Info = info;
		}
		public JSONArray getProtocolType() {
			return ProtocolType;
		}
		public void setProtocolType(JSONArray protocolType) {
			ProtocolType = protocolType;
		}
		public JSONArray getCdrType() {
			return CdrType;
		}
		public void setCdrType(JSONArray cdrType) {
			CdrType = cdrType;
		}
		public String getCaq() {
			return caq;
		}
		public void setCaq(String caq) {
			this.caq = caq;
		}
		public int getNetElem() {
			return netElem;
		}
		public void setNetElem(int netElem) {
			this.netElem = netElem;
		}
		public JSONArray getNetElemIdList() {
			return netElemIdList;
		}
		public void setNetElemIdList(JSONArray netElemIdList) {
			this.netElemIdList = netElemIdList;
		}
		public String getTacElemList() {
			String tacElemListStr="";
			//局向设置
			if(tacElemList!=null && tacElemList.size()>0){
				tacElemListStr=tacElemList.toString().replace("[", "").replace("]", "");
			}
			return tacElemListStr;
		}
		public void setTacElemList(JSONArray tacElemList) {
			this.tacElemList = tacElemList;
		}
		public JSONArray getPairElemList() {
			return pairElemList;
		}
		public void setPairElemList(JSONArray pairElemList) {
			this.pairElemList = pairElemList;
		}
		public JSONArray getPairIpList() {
			return pairIpList;
		}
		public void setPairIpList(JSONArray pairIpList) {
			this.pairIpList = pairIpList;
		}
		public JSONArray getTimeRangeList() {
			return timeRangeList;
		}
		public void setTimeRangeList(JSONArray timeRangeList) {
			this.timeRangeList = timeRangeList;
		}
		public int getTimeRank() {
			return timeRank;
		}
		public void setTimeRank(int timeRank) {
			this.timeRank = timeRank;
		}
		public int getStaticDim() {
			return staticDim;
		}
		public void setStaticDim(int staticDim) {
			this.staticDim = staticDim;
		}
		public JSONArray getIMEIList() {
			return IMEIList;
		}
		public void setIMEIList(JSONArray list) {
			IMEIList = list;
		}
		public String getSelectedKPI() {
			return selectedKPI;
		}
		public void setSelectedKPI(String selectedKPI) {
			this.selectedKPI = selectedKPI;
		}
		public JSONArray getKpiList() {
			return kpiList;
		}
		public void setKpiList(JSONArray kpiList) {
			this.kpiList = kpiList;
		}
		public JSONArray getM_Provinces() {
			return m_Provinces;
		}
		public void setM_Provinces(JSONArray provinces) {
			m_Provinces = provinces;
		}
		public JSONArray getM_Cities() {
			return m_Cities;
		}
		public void setM_Cities(JSONArray cities) {
			m_Cities = cities;
		}
		public JSONArray getM_MSCs() {
			return m_MSCs;
		}
		public void setM_MSCs(JSONArray cs) {
			m_MSCs = cs;
		}
		public JSONArray getSurSpcList() {
			return surSpcList;
		}
		public void setSurSpcList(JSONArray surSpcList) {
			this.surSpcList = surSpcList;
		}
		public JSONArray getDestSpcList() {
			return destSpcList;
		}
		public void setDestSpcList(JSONArray destSpcList) {
			this.destSpcList = destSpcList;
		}
		public int getM_sumType() {
			return m_sumType;
		}
		public void setM_sumType(int type) {
			m_sumType = type;
		}
		public String getShowType() {
			return ShowType;
		}
		public void setShowType(String showType) {
			ShowType = showType;
		}
		public String getOtherFilter() {
			return OtherFilter;
		}
		public void setOtherFilter(String otherFilter) {
			OtherFilter = otherFilter;
		}
		public boolean isISGSM() {
			return ISGSM;
		}
		public void setISGSM(boolean isgsm) {
			ISGSM = isgsm;
		}
		public boolean isISTD() {
			return ISTD;
		}
		public void setISTD(boolean istd) {
			ISTD = istd;
		}
		public JSONObject getTimeRange() {
			return timeRange;
		}
		public void setTimeRange(JSONObject timeRange) {
			this.timeRange = timeRange;
		}
		public String getTableName() {
			//局向设置
			if(ProtocolType!=null && ProtocolType.size()>0){
				tableName=ProtocolType.toString().replace("[", "").replace("]", "");
			}
			return tableName;
		}
		public void setTableName(String tableName) {
			this.tableName = tableName;
		}

		public String getOpc_dpc() {
			//局向设置
			if(pairElemList!=null && pairElemList.size()>0){
				opc_dpc=pairElemList.toString().replace("[", "").replace("]", "");
			}
			return opc_dpc;
		}

		public void setOpc_dpc(String opc_dpc) {
			this.opc_dpc = opc_dpc;
		}

		public String getNetElemId() {
			//网元设置
			if(netElemIdList!=null && netElemIdList.size()>0){
				netElemId = netElemIdList.toString().replace("[", "").replace("]", "");
			}
			return netElemId;
		}

		public void setNetElemId(String netElemId) {
			this.netElemId = netElemId;
		}
		
		public int getTableType() {
			if (ProtocolType != null && ProtocolType.size() > 0) {
				tableName = ProtocolType.toString().replace("[", "").replace("]",
						"");
			}
			if ("BSSAP".equals(tableName))
				tableType = 0;
			else if ("RANAP".equals(tableName))
				tableType = 1;
			else if ("BICC".equals(tableName))
				tableType = 2;
			return tableType;
		}

		public String getCompanyId() {
			if (tacElemList  != null && tacElemList .size() > 0) {
				companyId = tacElemList .toString().replace("[", "").replace("]",
						"");
			}
			return companyId;
		}

		public void setCompanyId(String companyId) {
			this.companyId = companyId;
		}

		public void setTableType(int tableType) {
			this.tableType = tableType;
		}

//		public static void main(String[] args){
//			String para = "{"+
//			"CallType:0,"+
//			"CDRCallType:2,"+
//			"CallTypeNum:'10086',"+
//			"Info:'1',BeginNum:1,OnceCount:1000,"+
//			"ProtocolType:['BSSAP'],"+
//			"CdrType:[1,2,3,4],"+
//			"caq:null,netElem:1,netElemIdList:[123,456,778],tacElemList:[],pairElemList:[22,33,44,55],pairIpList:[],"+
//			"timeRangeList:[{'nBeginTime':1301587200,'nEndTime':1304092800},{nBeginTime:1309622400,nEndTime:1309968000}],timeRank:0,staticDim:0,IMEIList:[]," +
//			"selectedKPI:null,kpiList:[],m_Provinces:[],m_Cities:[],m_MSCs:[],surSpcList:[],destSpcList:[],m_sumType:0,"+
//			"ShowType:null,OtherFilter:null,ISGSM:false,ISTD:false,"+
//			"timeRange:{}"+
//			"}";
//	        //2.定义表信息(通过字段获取之)，将json字段信息解析出来
//	    	CDRBeanForIndex cdr=new CDRBeanForIndex(JSONObject.fromObject(para));
//	    	System.out.println("主叫:"+cdr.getCalling_number());
//	    	System.out.println("被叫:"+cdr.getCalled_number());
//	    	System.out.println("cdr_type:"+cdr.getCdr_type());
//	    	System.out.println("网元类型:"+cdr.getNetElem());
//	    	System.out.println("网元设置数据:"+cdr.getNetElemId());
//	    	System.out.println("局向设置数据:"+cdr.getOpc_dpc());
//	    	long[] list = cdr.getStart_time_s();
//	    	long[] list1 = cdr.getEnd_time_s();
//			for (int i = 0; i < list.length; i++) {
//				System.out.println("开始时间:" + list[i]);
//			}
//			for (int i = 0; i < list1.length; i++) {
//				System.out.println("结束时间:" + list1[i]);
//			}
//	    	System.out.println("开始数目:"+cdr.getBeginNum());
//	    	System.out.println("查询数目:"+cdr.getOnceCount());
//	    	System.out.println("表名:"+cdr.getTableName());
//	    	System.out.println("表类型:"+cdr.getTableType());
//		}
		
		
	}

	/**
	 * @param args
	 * @throws SocketException 
	 */
	public static void main(String[] args) throws SocketException{
		// TODO Auto-generated method stub
		Enumeration netInterfaces=NetworkInterface.getNetworkInterfaces();   
		 InetAddress ip = null;   
		 while(netInterfaces.hasMoreElements())   
		 {   
		     NetworkInterface ni=(NetworkInterface)netInterfaces.nextElement();   
		     System.out.println(ni.getName());   
		     ip=(InetAddress) ni.getInetAddresses().nextElement();   
             System.out.println("IP:"
                         + ip.getHostAddress());
		     if( !ip.isSiteLocalAddress()   
		             && !ip.isLoopbackAddress()   
		             && ip.getHostAddress().indexOf(":")==-1)   
		     {   
		        System.out.println("本机的ip=" + ip.getHostAddress());   
		         break;   
		     }   
		     else  
		     {   
		         ip=null;   
		     }   
		 }  
		  

	}public static void main8(String[] args) {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
            netInterfaces = NetworkInterface.getNetworkInterfaces();

            while (netInterfaces.hasMoreElements()) {

                NetworkInterface ni = netInterfaces.nextElement();
                System.out.println("DisplayName:" + ni.getDisplayName());
                System.out.println("Name:" + ni.getName());

                Enumeration<InetAddress> ips = ni.getInetAddresses();
                while (ips.hasMoreElements()) {
                	InetAddress ip=(InetAddress) ni.getInetAddresses().nextElement(); 
                	
                    System.out.println("IP:"
                            + ip.getHostAddress());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
