package TableTest;

public class User {
	public static void main(String[] args) {
		try {
			String tableName = "users";
			String[] families = {"info"};
		    TableManipulator.createTable(tableName, families);
		    TableManipulator.addRecord(tableName, "TheRealMT", "info", "name", "Mark Twain");
		    TableManipulator.addRecord(tableName, "TheRealMT", "info", "email", "markTwain@gmail.com");
		    TableManipulator.addRecord(tableName, "TheRealMT", "info", "psw", "Longhorne");;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
