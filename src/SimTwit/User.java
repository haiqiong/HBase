package SimTwit;

/**
 * User data model.
 * @author haiqiongyao
 * Feb 23, 2013
 */

public class User {
	public String user;
	public String name;
	public String email;
	public String password;
	
	public User(String user, String name, String email, String password) {
		this.user = user;
		this.name = name;
		this.email = email;
		this.password = password;
	}
	
	@Override
	public String toString() {
		return String.format("<User: %s, %s, %s>", user, name, email);
	}
}
