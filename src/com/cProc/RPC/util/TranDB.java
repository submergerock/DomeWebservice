package com.cProc.RPC.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class TranDB {

	/**
	 * 保存total
	 */
	public static final ConcurrentMap<String, AtomicInteger> TOTAL_MAP = new ConcurrentHashMap<String, AtomicInteger>();

	/**
	 * 记录以及发送的数据
	 */
	public static final ConcurrentMap<String, AtomicInteger> SEND_MAP= new ConcurrentHashMap<String, AtomicInteger>();
	
	/**
	 * 记录受到的数据
	 */
	
	public static final ConcurrentMap<String,CopyOnWriteArrayList<String>> DATA_MAP= new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>();
	
	
	
}
