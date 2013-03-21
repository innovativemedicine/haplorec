package haplorec.util;

import groovy.sql.Sql

public class Haplotype {
	static def snpsToHaplotypes(String url, String username, String password, String driver = "com.mysql.jdbc.Driver") {
		return snpsToHaplotypes(Sql.newInstance(url, username, password, driver))
	}
	
	static def snpsToHaplotypes(Sql sql) {
		
	}
}
