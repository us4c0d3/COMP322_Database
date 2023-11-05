package lab7; // package name

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*; // import JDBC package
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC program for Database(comp322) Lab #7 Assignment
 * @author Jang Wooseok
 */
public class Lab7JDBC {
	public static final String URL = "jdbc:oracle:thin:@localhost:1521:orcl";
	public static final String USER_UNIVERSITY ="university";
	public static final String USER_PASSWD ="comp322";
	
	public static void main(String[] args) {
		Connection conn = null; // Connection object
		Statement stmt = null;	// Statement object

		try {
			// Load a JDBC driver for Oracle DBMS
			Class.forName("oracle.jdbc.driver.OracleDriver");
			// Get a Connection object
			System.out.println("Driver Loading: Success!");
		}catch(ClassNotFoundException e) {
			System.err.println("error = " + e.getMessage());
			System.exit(1);
		}

		// Make a conn
		try{
			conn = DriverManager.getConnection(URL, USER_UNIVERSITY, USER_PASSWD);
			System.out.println("Oracle Connected.");
		}catch(SQLException ex) {
			ex.printStackTrace();
			System.err.println("Cannot get a conn: " + ex.getLocalizedMessage());
			System.err.println("Cannot get a conn: " + ex.getMessage());
			System.exit(1);
		}
		try {
			stmt = conn.createStatement();

			doTask1(conn, stmt);

			System.out.println();

			doTask2(conn, stmt);
		} catch(SQLException e) {
			System.err.println("SQL error = " + e.getMessage());
			System.exit(1);
		}

		// Release database resources.
		try {
			// Close the Statement object.
			stmt.close(); 
			// Close the Connection object.
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void doTask1(Connection conn, Statement stmt) {
		// Fill out your code.
		try {
			conn.setAutoCommit(false);
			// Drop All tables
			// dropTables(conn, stmt);
			// Create Tables
			createTables(conn, stmt);

			// Insert Data
			insertData(conn, stmt, "company.txt");

		} catch(SQLException ex) {
			System.err.println("sql error = " + ex.getMessage());
			System.exit(1);
		}
	}

	private static void insertData(Connection conn, Statement stmt, String filename) {
		List<String> lines = readFile(filename);
		try {
			for(String line : lines) {
				String[] values = line.split("#");
				String sql = "";
				switch (values[0]) {
					case "EMPLOYEE":
						sql = String.format("INSERT INTO EMPLOYEE VALUES ('%s', '%s', '%s', '%s', TO_DATE('%s', 'yyyy-mm-dd'), '%s', '%s', %s, '%s', %s)",
								values[1], values[2], values[3], values[4], values[5], values[6], values[7],
								values[8], values[9].equals("NULL") ? null : values[9], values[10]);
						break;
					case "DEPARTMENT":
						sql = String.format("INSERT INTO DEPARTMENT VALUES ('%s', %s, '%s', TO_DATE('%s', 'yyyy-mm-dd'))",
								values[1], values[2], values[3], values[4]);
						break;
					case "DEPT_LOCATIONS":
						sql = String.format("INSERT INTO DEPT_LOCATIONS VALUES (%s, '%s')",
								values[1], values[2]);
						break;
					case "PROJECT":
						sql = String.format("INSERT INTO PROJECT VALUES ('%s', %s, '%s', %s)",
								values[1], values[2], values[3], values[4]);
						break;
					case "WORKS_ON":
						sql = String.format("INSERT INTO WORKS_ON VALUES ('%s', %s, %s)",
								values[1], values[2], values[3]);
						break;
					case "DEPENDENT":
						sql = String.format("INSERT INTO DEPENDENT VALUES ('%s', '%s', '%s', TO_DATE('%s', 'yyyy-mm-dd'), '%s')",
								values[1], values[2], values[3], values[4], values[5]);
						break;
				}

				if(!sql.isEmpty()) {
					stmt.addBatch(sql);
				}
			}

			int[] count = stmt.executeBatch();
			System.out.println(count.length + " row inserted.");

			conn.commit();
			System.out.println("commited.");
		} catch(SQLException e) {
			System.err.println("SQL error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static List<String> readFile(String filename) {
		List<String> lines = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			String tableName = "";
			while((line = br.readLine()) != null) {
				line = line.trim();
				if(line.startsWith("$")) {
					tableName = line.substring(1);
				} else {
					lines.add(tableName + "#" + line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return lines;
	}

	private static void dropTables(Connection conn, Statement stmt) {
		String dropEmployeeSQL = "DROP TABLE EMPLOYEE CASCADE CONSTRAINT";
		String dropDepartmentSQL = "DROP TABLE DEPARTMENT CASCADE CONSTRAINT";
		String dropDept_LocationsSQL = "DROP TABLE DEPT_LOCATIONS CASCADE CONSTRAINT";
		String dropProjectSQL = "DROP TABLE PROJECT CASCADE CONSTRAINT";
		String dropWorks_onSQL = "DROP TABLE WORKS_ON CASCADE CONSTRAINT";
		String dropDependentSQL = "DROP TABLE DEPENDENT CASCADE CONSTRAINT";

		try {
			stmt.executeUpdate(dropEmployeeSQL);
			stmt.executeUpdate(dropDepartmentSQL);
			stmt.executeUpdate(dropDept_LocationsSQL);
			stmt.executeUpdate(dropProjectSQL);
			stmt.executeUpdate(dropWorks_onSQL);
			stmt.executeUpdate(dropDependentSQL);
			System.out.println("Successfully dropped all tables.");
		} catch(SQLException e) {
			System.err.println("SQL error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createTables(Connection conn, Statement stmt) {
		try{
			createEmployeeTable(conn, stmt);
			createDepartmentTable(conn, stmt);
			createDept_LocationTable(conn, stmt);
			createProjectTable(conn, stmt);
			createWorks_onTable(conn, stmt);
			createDependentTable(conn, stmt);
			conn.commit();
			System.out.println("commited");
		} catch(SQLException e) {
			System.err.println("SQL error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createEmployeeTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE EMPLOYEE (\n" +
				"    Fname           VARCHAR(15)     NOT NULL,\n" +
				"    Minit           CHAR,\n" +
				"    Lname           VARCHAR(15),\n" +
				"    Ssn             CHAR(9)         NOT NULL,\n" +
				"    Bdate           DATE,\n" +
				"    Address         VARCHAR(30),\n" +
				"    Sex             CHAR,\n" +
				"    Salary          NUMBER(10,2),\n" +
				"    Super_ssn       CHAR(9),\n" +
				"    Dno             NUMBER  DEFAULT 1 NOT NULL,\n" +
				"    PRIMARY KEY (Ssn)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Employee table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createDepartmentTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE DEPARTMENT (\n" +
				"\tDname           VARCHAR(15)             NOT NULL,\n" +
				"    Dnumber         NUMBER                  NOT NULL,\n" +
				"    Mgr_ssn         CHAR(9)                 DEFAULT '888665555' NOT NULL,\n" +
				"    Mgr_start_date  DATE,\n" +
				"    PRIMARY KEY (Dnumber),\n" +
				"    UNIQUE (Dname)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Department table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createDept_LocationTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE DEPT_LOCATIONS (\n" +
				"    Dnumber NUMBER  NOT NULL,\n" +
				"    Dlocation       VARCHAR(15)     NOT NULL,\n" +
				"    PRIMARY KEY(Dnumber, Dlocation)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Dept_Location table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createProjectTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE PROJECT (\n" +
				"    Pname   VARCHAR(20)     NOT NULL,\n" +
				"    Pnumber NUMBER          NOT NULL,\n" +
				"    Plocation       VARCHAR(15) NOT NULL,\n" +
				"    Dnum    NUMBER,\n" +
				"    PRIMARY KEY(Pnumber),\n" +
				"    UNIQUE(Pname)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Project table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createWorks_onTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE WORKS_ON (\n" +
				"    Essn    CHAR(9)         NOT NULL,\n" +
				"    Pno     NUMBER          NOT NULL,\n" +
				"    Hours   NUMBER(10,1),\n" +
				"    PRIMARY KEY(Essn, Pno)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Works_on table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}

	private static void createDependentTable(Connection conn, Statement stmt) {
		String createSQL = "CREATE TABLE DEPENDENT (\n" +
				"    Essn    CHAR(9)         NOT NULL,\n" +
				"    Dependent_name  VARCHAR(15)     NOT NULL,\n" +
				"    Sex     CHAR,\n" +
				"    Bdate   DATE,\n" +
				"    Relationship    VARCHAR(15),\n" +
				"    PRIMARY KEY(Essn, Dependent_name)\n" +
				")";

		try {
			stmt.executeUpdate(createSQL);
			System.out.println("Dependent table created.");
		} catch(SQLException e) {
			System.err.println("sql error = " + e.getMessage());
			System.exit(1);
		}
	}
	
	public static void doTask2(Connection conn, Statement stmt) {
		ResultSet rs = null;
		try {
			// Q1: Complete your query.
			String sql = "SELECT\n" +
					"    E.Sex as Gender,\n" +
					"    ROUND(AVG(E.Salary), 2) as Avg_Sal\n" +
					"FROM\n" +
					"    EMPLOYEE E\n" +
					"JOIN\n" +
					"    DEPENDENT D on E.Ssn = D.Essn\n" +
					"WHERE\n" +
					"    D.Relationship = 'Son'\n" +
					"    OR D.Relationship = 'Daughter'\n" +
					"GROUP BY\n" +
					"    E.Sex\n" +
					"ORDER BY\n" +
					"    AVG(E.Salary) DESC";
			rs = stmt.executeQuery(sql);
			System.out.println("<< query 1 result >>");
			System.out.printf("%-9s | %s\n", "Gender", "Avg_Sal");
			System.out.println("---------------------");
			if(rs != null) {
				while(rs.next()) {
					String gender = rs.getString(1);
					double avgSal = rs.getDouble(2);
					System.out.printf("%-9s | %.2f\n", gender, avgSal);
				}
			} else {
				System.out.println("no rows returned.");
			}

			rs.close();
			
			System.out.println();
			
			// Q2: Complete your query.
			sql = "SELECT \n" +
					"    E.Fname, \n" +
					"    E.Lname, \n" +
					"    E.Address, \n" +
					"    S.Fname, \n" +
					"    S.Lname\n" +
					"FROM \n" +
					"    EMPLOYEE E\n" +
					"JOIN \n" +
					"    EMPLOYEE S ON E.Super_ssn = S.Ssn\n" +
					"WHERE \n" +
					"    NOT EXISTS (\n" +
					"        SELECT P.Pnumber \n" +
					"        FROM PROJECT P\n" +
					"        WHERE P.Dnum = 1\n" +
					"        AND NOT EXISTS (\n" +
					"            SELECT W.Pno \n" +
					"            FROM WORKS_ON W\n" +
					"            WHERE W.Pno = P.Pnumber \n" +
					"            AND W.Essn = E.Ssn\n" +
					"        )\n" +
					"    )\n" +
					"ORDER BY \n" +
					"    E.Address ASC";
			rs = stmt.executeQuery(sql);
			System.out.println("<< query 2 result >>");
			System.out.printf("%-9s | %-12s | %-30s | %-12s | %s\n", "Fname", "Lname", "E_Address", "Super_Fname", "Super_Lname");
			System.out.println("-----------------------------------------------------------------------------------------");
			if(rs != null) {
				while(rs.next()) {
					String fname = rs.getString(1);
					String lname = rs.getString(2);
					String addr = rs.getString(3);
					String sfname = rs.getString(4);
					String slname = rs.getString(5);

					System.out.printf("%-9s | %-12s | %-30s | %-12s | %s\n", fname, lname, addr, sfname, slname);
				}
			} else {
				System.out.println("no rows returned.");
			}
			rs.close();

			System.out.println();

			// Q3: Complete your query.
			sql = "SELECT\n" +
					"    D.Dname,\n" +
					"    P.Pname,\n" +
					"    E.Lname,\n" +
					"    E.Fname,\n" +
					"    E.Salary\n" +
					"FROM\n" +
					"    DEPARTMENT D\n" +
					"FULL OUTER JOIN\n" +
					"    PROJECT P ON D.Dnumber = P.Dnum\n" +
					"FULL OUTER JOIN\n" +
					"    EMPLOYEE E ON D.Dnumber = E.Dno\n" +
					"WHERE\n" +
					"    P.Plocation = 'Houston'\n" +
					"ORDER BY\n" +
					"    D.Dname ASC,\n" +
					"    E.Salary DESC";
			rs = stmt.executeQuery(sql);
			System.out.println("<< query 3 result >>");
			System.out.printf("%-15s | %-15s | %-12s | %-12s | %s\n", "Dname", "Pname", "Lname", "Fname", "Salary");
			System.out.println("------------------------------------------------------------------------");
			if(rs != null) {
				while(rs.next()) {
					String dname = rs.getString(1);
					String pname = rs.getString(2);
					String elname = rs.getString(3);
					String efname = rs.getString(4);
					String esalary = rs.getString(5);

					System.out.printf("%-15s | %-15s | %-12s | %-12s | %s\n", dname, pname, elname, efname, esalary);
				}
			} else {
				System.out.println("no rows returned.");
			}

			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
