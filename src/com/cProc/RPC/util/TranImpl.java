package com.cProc.RPC.util;

import java.util.ArrayList;

public class TranImpl implements ITran{
	private int i = 0;
	
	public boolean sendSize(int size, String sessionId) {
		// TODO Auto-generated method stub
		System.out.println(i++);
		TranDB.TOTAL_MAP.get(sessionId).addAndGet(size);
		
		return true;
	}

	@Override
	public boolean sendList(ArrayList listValue, String sessionId) {
		// TODO Auto-generated method stub
		System.out.println(i++);
		//DB.DATA_MAP.get(sessionId).addAll(listValue);
		return true;
	}
	

}
