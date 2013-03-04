package TableTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Connector {
	
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		
		HBaseAdmin admin = new HBaseAdmin(config);
		String tableName = "testHTable1";
		String columnFamily = "course";
		
		if (admin.tableExists(tableName)) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(tableDesc);
		}
		
		HTable table = new HTable(config, tableName);
		Put p = new Put(Bytes.toBytes("testRow"));
		p.add(Bytes.toBytes("course"), Bytes.toBytes("history"), 
		Bytes.toBytes("85"));
		table.put(p);
		System.out.println("insert record");
		
		Get g = new Get(Bytes.toBytes("testRow"));
		Result  r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("course"), Bytes.toBytes("history"));
		String valueStr = Bytes.toString(value);
		System.out.println("Get: " + valueStr);
		
		//use scan.
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("course"), Bytes.toBytes("firstRow"));
		ResultScanner scanner = table.getScanner(s);
		try {
			for (Result t = scanner.next(); t != null; t = scanner.next()) {
				System.out.println("Found row: " + t);
			}
		} finally {
			scanner.close();
		}
	}
}
