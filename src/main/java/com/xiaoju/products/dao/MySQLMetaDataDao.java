package com.xiaoju.products.dao;

import com.xiaoju.products.bean.ColumnNode;
import com.xiaoju.products.bean.TableNode;
import com.xiaoju.products.exception.DBException;
import com.xiaoju.products.util.Check;
import com.xiaoju.products.util.DBUtil;
import com.xiaoju.products.util.DBUtil.DB_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 元数据dao
 * @author yangyangthomas
 */
public class MySQLMetaDataDao {
	DBUtil dbUtil = new DBUtil(DB_TYPE.META);
	
	
	public List<ColumnNode> getColumn(String db, String table){
    	String sqlWhere  = "1 = 1" + (Check.isEmpty(db) ? " " : (" and name='"+db+"'"));
    	List<ColumnNode> colList = new ArrayList<ColumnNode>();
		String sql = "select columns.`COLUMN_NAME` as column_name, columns.`INTEGER_IDX` as column_index, merges2.table_id, merges2.table_name, merges2.db_name, merges2.db_id  from `COLUMNS_V2` columns \n" +
				"join  (select `CD_ID`, merges.* from `sds` sds1 \n" +
				"  join (SELECT `SD_ID` ,`TBL_ID` as table_id, `TBL_NAME` as table_name, db.`NAME` as db_name, db.`DB_ID` as db_id FROM `TBLS` tb \n" +
				"    join (SELECT `NAME`, `DB_ID` from `DBS`) db \n" +
				"    on (tb.`DB_ID`=db.`DB_ID`) where tb.`TBL_NAME`='%s' and db.`NAME` ='%s' ) merges \n" +
				"  on (sds1.`SD_ID` = merges.`SD_ID`)) merges2 \n" +
				"on columns.`CD_ID` = merges2.`CD_ID`   order by columns.`INTEGER_IDX`";
		String repalceSql = String.format(sql, table, db);
    	
		try {
			List<Map<String, Object>> rs = dbUtil.doSelect(repalceSql);
			for (Map<String, Object> map : rs) {
				ColumnNode column = new ColumnNode();
				Long tableId =  Long.valueOf(map.get("table_id").toString());
				Long dbId = Long.valueOf(map.get("db_id").toString());
				Long columnIndex = Long.valueOf(map.get("column_index").toString());
				String columnId = String.format("%s_%s_%s", tableId, dbId, columnIndex);
				column.setId(columnId);
				column.setColumn((String) map.get("column_name"));
				column.setTableId(tableId);
				column.setTable((String) map.get("table_name"));
				column.setDb((String) map.get("db_name"));
				colList.add(column);
			}
	    	return colList;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBException(repalceSql, e);
		}
    }
	
	public List<TableNode> getTable(String db, String table){
    	String sqlWhere  = "is_effective=1 and data_name='" + table + "'" + (Check.isEmpty(db) ? " " : (" and datastorage_name='"+db+"'"));
    	List<TableNode> list = new ArrayList<TableNode>();
    	String sql = "SELECT data_id,data_name,datastorage_name from r_data where " + sqlWhere + "";
		try {
			List<Map<String, Object>> rs = dbUtil.doSelect(sql);
			for (Map<String, Object> map : rs) {
				TableNode tableNode = new TableNode();
				tableNode.setId((Long) map.get("data_id"));
				tableNode.setTable((String) map.get("data_name"));
				tableNode.setDb((String) map.get("datastorage_name"));
				list.add(tableNode);
			}
	    	return list;
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBException(sqlWhere, e);
		}
    }
	
}
