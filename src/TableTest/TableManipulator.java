package TableTest;

/**
 * @author haiqiongyao
 * Feb 22, 2013
 * 1. create table with TablePool rather than HBTable.
 * Creating a table instance is an expensive operation, requiring a bit of 
 * network overhead.
 * 
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TableManipulator {
	private static Configuration conf = null;
	
	static {
		conf = HBaseConfiguration.create();
	}
	
	//create a table.
	public static void createTable(String tableName, String[] families) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists.");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (String family : families) {
				tableDesc.addFamily(new HColumnDescriptor(family));
			}
			admin.createTable(tableDesc);
		}
	}
	
	public static void deleteTable(String tableName) throws Exception {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}
		
	public static void addRecord(String tableName, String rowKey, 
			String family, String qualifier, String value) throws Exception{
		HTableInterface table = null;
		try {
//			HTable table = new HTable(conf, tableName);
			HTablePool pool = new HTablePool();
			table = pool.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), 
					Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}
	
	public static void delRecord(String tableName, String rowKey) throws IOException {
		HTablePool pool = new HTablePool();
		HTableInterface table = pool.getTable(tableName);
//		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		table.close();
	}
	
	public static void getOneRecord(String tableName, String rowKey) throws IOException {
//		HTable table = new HTable(conf, tableName);
		HTablePool pool = new HTablePool();
		HTableInterface table = pool.getTable(tableName);
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		for (KeyValue kv:result.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + " ");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(new String(kv.getValue()) + " ");
			System.out.println(kv.getTimestamp());
		}
		table.close();
	}
	
	public static void getAllRecord(String tableName)  throws IOException{
		ResultScanner rs = null;
		HTableInterface table = null;
		try {
//			HTable table = new HTable(conf, tableName);
			HTablePool pool = new HTablePool();
			table = pool.getTable(tableName);
			Scan s = new Scan();
			rs = table.getScanner(s);
			for (Result result = rs.next(); result != null; result = rs.next()) {
				for (KeyValue kv : result.raw()) {
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + " ");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(new String(kv.getValue()) + " ");
					System.out.println(kv.getTimestamp());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			rs.close();
			table.close();
		}
	}
	
	public static void main(String[] args) {
		try {
			String tableName = "scores";
			String[] families = {"grade", "course"};
		    TableManipulator.createTable(tableName, families);
		    
		    TableManipulator.addRecord(tableName, "ami", "grade", "", "5");
		    TableManipulator.addRecord(tableName, "ami", "course", "math", "85");
		    TableManipulator.addRecord(tableName, "ami", "course", "history", "95");
		    TableManipulator.addRecord(tableName, "ami", "course", "", "75");
		    
		    TableManipulator.addRecord(tableName, "bob", "grade", "", "4");
		    TableManipulator.addRecord(tableName, "bob", "course", "math", "82");
		    
		    System.out.println("one record of ami");
		    TableManipulator.getOneRecord(tableName, "ami");
		    System.out.println("all records");
		    TableManipulator.getAllRecord(tableName);
		    System.out.println("del one record of bob");
		    TableManipulator.delRecord(tableName, "bob");
		    System.out.println("all records");
		    TableManipulator.getAllRecord(tableName);
		    System.out.println("del all records");
		    TableManipulator.deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
