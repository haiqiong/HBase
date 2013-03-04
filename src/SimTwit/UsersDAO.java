package SimTwit;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class UsersDAO {
	public static final byte[] TABLE_NAME = Bytes.toBytes("users");
	public static final byte[] INFO_FAM = Bytes.toBytes("info");
	public static final byte[] USER_COL = Bytes.toBytes("user");
	public static final byte[] NAME_COL = Bytes.toBytes("name");
	public static final byte[] EMAIL_COL = Bytes.toBytes("email");
	public static final byte[] PASS_COL = Bytes.toBytes("passwrod");
	public static final byte[] TWITCOUNT_COL = Bytes.toBytes("twit_count");
	
	private HTablePool pool;
	
	public UsersDAO(HTablePool pool) {
		this.pool = pool;
	}
	
	/**
	 * Get op on user name + col_family
	 * @param user
	 * @return
	 */
	private static Get get(String user) {
		Get g = new Get(Bytes.toBytes(user));
		g.addFamily(INFO_FAM);
		return g;
	}
	
	/**
	 * Put all fields of user obj to column qualifiers.
	 * @param u
	 * @return
	 */
	private static Put put(User u) {
		Put p = new Put(Bytes.toBytes(u.name));
		p.add(INFO_FAM, USER_COL, Bytes.toBytes(u.user));
		p.add(INFO_FAM, NAME_COL, Bytes.toBytes(u.name));
		p.add(INFO_FAM, EMAIL_COL, Bytes.toBytes(u.email));
		p.add(INFO_FAM, PASS_COL, Bytes.toBytes(u.password));
		return p;
	}
	
	/**
	 * delete op on user name;
	 * @param user
	 * @return
	 */
	private static Delete del(String user) {
		Delete d = new Delete(Bytes.toBytes(user));
		return d;
	}
	
	public void addUser(String user, String name, 
			String email, String password) throws IOException{
		HTableInterface userTable = pool.getTable(TABLE_NAME);
		Put p = put(new User(user, name, email, password));
		userTable.put(p);
		userTable.close();
	}
	
//	public User getUser(String user) {
//		HTableInterface userTable = pool.getTable(TABLE_NAME);
//		Result selectedUser = userTable.get(get(user));
//		
//	}
}
