package SimTwit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class TwitDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
	public static final byte[] TWITS_FAM = Bytes.toBytes("twits");
	public static final byte[] USER_COL = Bytes.toBytes("user");
	public static final byte[] TWIT_COL = Bytes.toBytes("twit");
	public static final byte[] TIME_COL = Bytes.toBytes("time");
	public static final int LONG_LEN = Long.SIZE / 8;
	
	public void createTable() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(TABLE_NAME)) {
			System.out.println("table exists.");
		} else {
			HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
			HColumnDescriptor colDesc = new HColumnDescriptor(TWITS_FAM);
			colDesc.setMaxVersions(1);
			desc.addFamily(colDesc);
			admin.createTable(desc);
		}
	}
	
	/**
	 * rowKey = MD5 hash(username) + timestamp. The twits from a same user
	 * are grouped in a bucket and twits are sorted by desc.
	 * @param user
	 * @param message
	 * @param timestamp
	 */
	private Put put(String user, String message, Long timestamp) {
		byte[] userHash = Md5Utils.md5sum(user);
		byte[] time = Bytes.toBytes(-1 * timestamp);
		byte[] rowKey = new byte[Md5Utils.MD5_LENGTH + LONG_LEN];
		int offset = 0;
		offset = Bytes.putBytes(rowKey, offset, userHash, 0, userHash.length);
		Bytes.putBytes(rowKey, offset, time, 0, time.length);
		Put p = new Put(rowKey);
		p.add(TWITS_FAM, TWIT_COL, Bytes.toBytes(message));
		p.add(TWITS_FAM, TIME_COL, Bytes.toBytes(user));
		return p;
	}
	
	private Scan getScan(String user) {
		byte[] userHash = Md5Utils.md5sum(user);
		byte[] startRow = Bytes.padTail(userHash, LONG_LEN);
		byte[] endRow = Bytes.padTail(userHash, LONG_LEN);
		endRow[Md5Utils.MD5_LENGTH - 1]++;
		return new Scan(startRow, endRow);
	}
	
	/**
	 * filter the twit contains str.
	 * @param str
	 * @return
	 */
	private Scan getFilterScan(String str) {
		Scan s = new Scan();
		s.addColumn(TWITS_FAM, TWIT_COL);
		Filter f = null;
//		Filter f = new ValueFilter(CompareFilter.CompareOp.EQUAL, 
//				new RegexStringComparator(".*" + str + ".*"));
		s.setFilter(f);
		return s;
	}
	
	public List<Twit> getTwits(String user) throws Exception{
		HTableInterface table = null;
		Scan s = getScan(user);
		try {
			HTablePool pool = new HTablePool();
			table = pool.getTable(TABLE_NAME);
			ResultScanner rs = table.getScanner(s);
			List<Twit> twitList = new ArrayList<Twit>();
			for (Result r : rs) {
				String userName = Bytes.toString(r.getValue(TWITS_FAM, USER_COL));
				String twit = Bytes.toString(r.getValue(TWITS_FAM, TWIT_COL));
				byte[] b = Arrays.copyOfRange(r.getRow(), Md5Utils.MD5_LENGTH, 
						Md5Utils.MD5_LENGTH + LONG_LEN);
				Date time = new Date(-1 * Bytes.toLong(b));
				twitList.add(new Twit(userName, twit, time));
			}
			return twitList;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
		return null;
	}
}
