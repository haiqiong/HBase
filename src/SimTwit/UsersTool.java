package SimTwit;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;

public class UsersTool {
	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.exit(0);
		}
		HTablePool pool = new HTablePool();
		UsersDAO dao = new UsersDAO(pool);
		
		if ("get".equals(args[0])) {
			System.out.println("Get user " + args[1]);
		}
	}
}
