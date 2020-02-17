package com.xiaoju.products.util;

import java.util.*;

import com.xiaoju.products.bean.ColumnNode;
import com.xiaoju.products.dao.MetaDataDao;

public class MetaCache {
	private static MetaCache instance = new MetaCache();
	
	public static MetaCache getInstance(){
		return instance;
	}
	
	private MetaCache(){}

	private MetaDataDao dao = new MetaDataDao();
	private static Map<String, List<ColumnNode>> cMap = new HashMap<String, List<ColumnNode>>();
	private static Map<String, Long> tableMap = new HashMap<String, Long>();
	private static Map<String, String> columnMap = new HashMap<String, String>();

	public void init(String table, List<String> usedColumns){
		String[] pdt = ParseUtil.parseDBTable(table);
		List<ColumnNode> list = dao.getColumn(pdt[0], pdt[1]);
		List<ColumnNode> filterList = new ArrayList<>(list.size());
		if (Check.notEmpty(list)) {
			if(!usedColumns.isEmpty()){
				for(String columnName : usedColumns){
					ColumnNode columnNode = list.stream().filter(cl -> cl.getColumn().toLowerCase().equals(columnName.toLowerCase()))
							.findFirst().orElse(null);
					if(columnNode != null){
						filterList.add(columnNode);
					}
				}
				if(filterList.size() < usedColumns.size()){ //存在错误字段
					filterList = list;
				}
			} else {
				filterList = list;
			}
			cMap.put(table.toLowerCase(), filterList);
			tableMap.put(table.toLowerCase(), filterList.get(0).getTableId());
			for (ColumnNode cn : filterList) {
				columnMap.put((cn.getDb()+"."+cn.getTable()+"."+cn.getColumn()).toLowerCase(),cn.getId());
			}
		}
	}

	public void init(String table){
		this.init(table, Collections.emptyList());
	}
	
	public void release(){
		cMap.clear();
		tableMap.clear();
		columnMap.clear();
	}
	
	public List<String> getColumnByDBAndTable(String table){
		List<ColumnNode> list = cMap.get(table.toLowerCase());
		List<String> list2 = new ArrayList<String>();
		if (Check.notEmpty(list)) {
			for (ColumnNode columnNode : list) {
				list2.add(columnNode.getColumn());
			}
		}
		return list2;
	}
	
	public Map<String, List<ColumnNode>> getcMap() {
		return cMap;
	}

	public Map<String, Long> getTableMap() {
		return tableMap;
	}

	public Map<String, String> getColumnMap() {
		return columnMap;
	}
}
