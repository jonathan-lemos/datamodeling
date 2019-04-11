import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

public class Main {
	private final static String jdbcDatabase = "proj3";
	private final static String jdbcDriver = "com.mysql.jdbc.Driver";
	private final static String jdbcUrl = "jdbc:mysql://localhost:3306";
	private final static String jdbcUser = "root";
	private final static String jdbcPassword = "root";

	private static String fmtDate() {
		return fmtDate(new Date());
	}

	private static String fmtDate(Date d) {
		return new SimpleDateFormat("yyyy-MM-dd").format(d);
	}

	private static Connection makeConnection() throws ClassNotFoundException, SQLException {
		Connection con;
		Class.forName(jdbcDriver);
		System.setProperty("jdbc.drivers", jdbcDriver);

		con = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
		con.setAutoCommit(false);

		Statement s1 = con.createStatement();
		s1.executeUpdate("DROP DATABASE IF EXISTS " + jdbcDatabase + ";");
		s1.close();

		Statement s2 = con.createStatement();
		s2.executeUpdate("CREATE DATABASE  " + jdbcDatabase + ";");
		s2.close();

		con.setCatalog(jdbcDatabase);
		return con;
	}

	private static ResultSet query(Connection con, String query) throws SQLException {
		try {
			Statement s = con.createStatement();
			return s.executeQuery(query);
		}
		catch (SQLException e) {
			throw new SQLException("Bad query \"" + query + "\"\n" + e.getMessage());
		}
	}

	private static int update(Connection con, String query) throws SQLException {
		try {
			Statement s = con.createStatement();
			s.executeUpdate(query);
			return s.getUpdateCount();
		}
		catch (SQLException e) {
			throw new SQLException("Bad query \"" + query + "\"\n" + e.getMessage());
		}
	}

	private static void insertFromCsv(Connection con, String csvFname, boolean ignoreFirstLine, String tableName) throws SQLException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(csvFname));

		StringBuilder sb = new StringBuilder();

		String curLine;
		while ((curLine = br.readLine()) != null) {
			if (ignoreFirstLine) {
				ignoreFirstLine = false;
				continue;
			}

			String[] arr = (curLine + " ").split(",");
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = arr[i].trim();
				if (arr[i].equals("")) {
					arr[i] = "NULL";
				}
			}
			sb.append("(");
			sb.append(String.join(",", arr));
			sb.append("),");
		}
		sb.deleteCharAt(sb.length() - 1);
		br.close();

		update(con, "INSERT INTO " + tableName + " VALUES " + sb.toString() + ";");
	}

	private static void createTables(Connection con) throws SQLException, IOException {
		update(con,
				"CREATE TABLE IF NOT EXISTS players (" +
						"player_id INT NOT NULL, " +
						"tag VARCHAR(255) NOT NULL, " +
						"real_name VARCHAR(255) NOT NULL, " +
						"nationality CHAR(2) NOT NULL, " +
						"birthday DATE NOT NULL, " +
						"game_race CHAR(1) NOT NULL, " +
						"PRIMARY KEY (player_id));");
		insertFromCsv(con, "players.csv", false, "players");

		update(con,
				"CREATE TABLE IF NOT EXISTS teams (" +
						"team_id INT NOT NULL, " +
						"name VARCHAR(255) NOT NULL, " +
						"founded DATE NOT NULL, " +
						"disbanded DATE, " +
						"PRIMARY KEY (team_id));");
		insertFromCsv(con, "teams.csv", false, "teams");

		update(con,
				"CREATE TABLE IF NOT EXISTS members (" +
						"player INT NOT NULL, " +
						"team INT NOT NULL, " +
						"start_date DATE NOT NULL, " +
						"end_date DATE, " +
						"PRIMARY KEY (player, start_date), " +
						"FOREIGN KEY (player) REFERENCES players (player_id), " +
						"FOREIGN KEY (team) REFERENCES teams (team_id));");
		insertFromCsv(con, "members.csv", false, "members");

		update(con,
				"CREATE TABLE IF NOT EXISTS tournaments (" +
						"tournament_id INT NOT NULL, " +
						"name VARCHAR(255) NOT NULL, " +
						"region CHAR(2), " +
						"major BOOLEAN NOT NULL, " +
						"PRIMARY KEY (tournament_id));");
		insertFromCsv(con, "tournaments.csv", false, "tournaments");

		update(con,
				"CREATE TABLE IF NOT EXISTS matches (" +
						"match_id INT NOT NULL, " +
						"date DATE NOT NULL, " +
						"tournament INT NOT NULL, " +
						"playerA INT NOT NULL, " +
						"playerB INT NOT NULL, " +
						"scoreA INT NOT NULL, " +
						"scoreB INT NOT NULL, " +
						"offline BOOLEAN NOT NULL, " +
						"PRIMARY KEY (match_id), " +
						"FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id), " +
						"FOREIGN KEY (playerA) REFERENCES players (player_id), " +
						"FOREIGN KEY (playerB) REFERENCES players (player_id));");
		insertFromCsv(con, "matches_v2.csv", false, "matches");

		update(con,
				"CREATE TABLE IF NOT EXISTS earnings (" +
						"tournament INT NOT NULL, " +
						"player INT NOT NULL, " +
						"prize_money INT NOT NULL, " +
						"position INT NOT NULL, " +
						"PRIMARY KEY (tournament, player), " +
						"FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id), " +
						"FOREIGN KEY (player) REFERENCES players (player_id));");
		insertFromCsv(con, "earnings.csv", false, "earnings");
	}

	private static void query1(Connection con, int month, int year) throws SQLException {
		ResultSet rs = query(con,
				"SELECT real_name, tag, nationality " +
						"FROM players " +
						"WHERE MONTH(birthday) = " + month + " " +
						"AND YEAR(birthday) = " + year + ";"
		);
		System.out.println(String.format("%-25s%-20s%-10s", "Real Name", "Tag", "Nationality"));
		System.out.println(String.format("%-25s%-20s%-10s", "---------", "---", "-----------"));
		while (rs.next()) {
			String rn = rs.getString("real_name");
			String tag = rs.getString("tag");
			String nat = rs.getString("nationality");
			System.out.println(String.format("%-25s%-20s%-10s", rn, tag, nat));
		}
		rs.close();
	}

	private static void query2(Connection con, int player_id, int team_id) throws SQLException {
		String curDate = "'" + fmtDate() + "'";
		ResultSet rs = query(con,
				"SELECT * " +
						"FROM members " +
						"WHERE player = " + player_id + " " +
						"AND team = " + team_id + ";"
		);
		if (rs.next()) {
			System.out.println("Player " + player_id + " is already a member of team " + team_id);
			rs.close();
			return;
		}
		rs.close();

		rs = query(con, "SELECT * FROM players WHERE player_id = " + player_id);
		if (!rs.next()) {
			System.out.println("No player exists with the id " + player_id);
			return;
		}
		rs.close();

		rs = query(con, "SELECT * FROM teams WHERE team_id = " + team_id);
		if (!rs.next()) {
			System.out.println("No team exists with the id " + team_id);
			return;
		}
		rs.close();

		if (update(con,
				"UPDATE members " +
						"SET end_date = " + curDate + " " +
						"WHERE player = " + player_id + " " +
						"AND end_date IS NULL;"
		) > 0) {
			System.out.println("Player " + player_id + " departed from old team");
		}
		update(con,
				"INSERT INTO members VALUES (" +
						player_id + "," +
						team_id + "," +
						curDate + "," +
						"NULL) "
		);
		System.out.println("Inserted player " + player_id + " into team " + team_id);
		rs.close();
	}

	private static void query3(Connection con, String nationality, int year) throws SQLException {
		ResultSet rs = query(con,
				"SELECT real_name, birthday " +
						"FROM players " +
						"WHERE YEAR(birthday) = " + year + " " +
						"AND nationality = '" + nationality + "';"
		);
		System.out.println(String.format("%-25s%-20s", "Real Name", "Birthday"));
		System.out.println(String.format("%-25s%-20s", "---------", "--------"));
		while (rs.next()) {
			System.out.println(String.format("%-25s%-20s", rs.getString("real_name"), fmtDate(rs.getDate("birthday"))));
		}
		rs.close();
	}

	private static void query4(Connection con) throws SQLException {
		ResultSet rs = query(con,
				"SELECT q.tag, q.game_race " +
						"FROM (" +
						"SELECT p.player_id, p.tag, p.game_race, t.tournament_id, t.region, t.major, e.player, e.position " +
						"FROM tournaments t " +
						"INNER JOIN earnings e ON e.tournament = t.tournament_id " +
						"INNER JOIN players p ON e.player = p.player_id " +
						"WHERE t.major = true " +
						"AND e.position = 1" +
						") q " +
						"GROUP BY player_id " +
						"HAVING COUNT(CASE WHEN q.region = 'AM' THEN 1 END) > 0 " +
						"AND COUNT(CASE WHEN q.region = 'EU' THEN 1 END) > 0 " +
						"AND COUNT(CASE WHEN q.region = 'KR' THEN 1 END) > 0 " +
						"ORDER BY tag ASC;"
		);

		System.out.println(String.format("%-20s%-10s", "Tag", "Game Race"));
		System.out.println(String.format("%-20s%-10s", "---", "---------"));
		while (rs.next()) {
			String tag = rs.getString("tag");
			String gr = rs.getString("game_race");
			System.out.println(String.format("%-20s%-10s", tag, gr));
		}
		rs.close();
	}

	private static void query5(Connection con) throws SQLException {
		ResultSet rs = query(con,
				"SELECT p.tag, p.real_name, max(m.end_date) as ed " +
						"FROM members m " +
						"INNER JOIN teams t ON m.team = t.team_id and t.name = 'ROOT Gaming' " +
						"INNER JOIN players p ON m.player = p.player_id " +
						"GROUP BY player " +
						"HAVING COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) = COUNT(*) " +
						"ORDER BY player "
		);

		System.out.println(String.format("%-20s%-25s%-20s", "Tag", "Real Name", "Departed"));
		System.out.println(String.format("%-20s%-25s%-20s", "---", "---------", "--------"));
		while (rs.next()) {
			String tag = rs.getString("tag");
			String rn = rs.getString("real_name");
			String ed = fmtDate(rs.getDate("ed"));
			System.out.println(String.format("%-20s%-25s%-20s", tag, rn, ed));
		}
		rs.close();
	}

	private static void query6(Connection con) throws SQLException {
		ResultSet rs = query(con,
				"SELECT tag, nationality, count(CASE WHEN IF(player_id = playerA, scoreA > scoreB, scoreB > scoreA) THEN 1 END) / COUNT(*) * 100 AS win_pct " +
						"FROM (" +
						"SELECT p.player_id, p.tag, p.nationality, m.playerA, m.playerB, m.scoreA, m.scoreB " +
						"FROM players p, matches m " +
						"WHERE (m.playerA IN (SELECT player_id FROM players WHERE game_race = 'P') AND m.playerB IN (SELECT player_id FROM players WHERE game_race = 'T') AND p.player_id = m.playerA) " +
						"OR    (m.playerB IN (SELECT player_id FROM players WHERE game_race = 'P') AND m.playerA IN (SELECT player_id FROM players WHERE game_race = 'T') AND p.player_id = m.playerB) " +
						") t " +
						"GROUP BY tag " +
						"HAVING win_pct >= 65 " +
						"AND COUNT(CASE WHEN IF(t.player_id = t.playerA, t.scoreA > t.scoreB, t.scoreB > t.scoreA) then 1 end) >= 10 " +
						"ORDER BY win_pct DESC;"
		);
		System.out.println(String.format("%-20s%-15s%-10s", "Tag", "Nationality", "Win Percentage"));
		System.out.println(String.format("%-20s%-15s%-10s", "---", "-----------", "--------------"));
		while (rs.next()) {
			String tag = rs.getString("tag");
			String nat = rs.getString("nationality");
			Double pct = rs.getDouble("win_pct");
			System.out.println(String.format("%-20s%-15s%-10.4f", tag, nat, pct));
		}
		rs.close();
	}

	private static void query7(Connection con) throws SQLException {
		ResultSet rs = query(con,
				"SELECT t.name, founded, " +
						"COUNT(CASE WHEN p.game_race = 'P' THEN 1 END) AS p_cnt, " +
						"COUNT(CASE WHEN p.game_race = 'T' THEN 1 END) AS t_cnt, " +
						"COUNT(CASE WHEN p.game_race = 'Z' THEN 1 END) AS z_cnt " +
						"FROM teams t " +
						"INNER JOIN members m ON m.team = t.team_id " +
						"INNER JOIN players p ON p.player_id = m.player " +
						"WHERE YEAR(t.founded) < 2011 " +
						"AND t.disbanded IS NULL " +
						"GROUP BY t.name " +
						"ORDER BY t.name ASC "
		);
		System.out.println(String.format("%-20s%-15s%-15s%-15s%-15s", "Team Name", "Founded", "Protoss Count", "Terran Count", "Zerg Count"));
		System.out.println(String.format("%-20s%-15s%-15s%-15s%-15s", "---------", "-------", "-------------", "------------", "----------"));
		while (rs.next()) {
			String tag = rs.getString("name");
			String founded = fmtDate(rs.getDate("founded"));
			int p = rs.getInt("p_cnt");
			int t = rs.getInt("t_cnt");
			int z = rs.getInt("z_cnt");
			System.out.println(String.format("%-20s%-15s%-15d%-15d%-15d", tag, founded, p, t, z));
		}
	}

	private static int readInt(Scanner sc, String prompt) throws NumberFormatException {
		System.out.print(prompt);
		while (true) {
			try {
				String s = sc.nextLine().trim();
				return Integer.parseInt(s);
			}
			catch (NumberFormatException e) {
				System.out.println("Enter a number, dingus: ");
			}
		}
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		Scanner sc = new Scanner(System.in);
		Connection con = makeConnection();
		System.out.println("BIG DATA INSERTING...");
		createTables(con);
		System.out.println("BIG DATA INSERTED!");
		System.out.println();

		try {
			while (true) {
				System.out.println("1. Query 1");
				System.out.println("2. Query 2");
				System.out.println("3. Query 3");
				System.out.println("4. Query 4");
				System.out.println("5. Query 5");
				System.out.println("6. Query 6");
				System.out.println("7. Query 7");
				System.out.println("8. Exit");

				int choice = readInt(sc, "Make a selection: ");

				switch (choice) {
					case 1:
						int year = readInt(sc, "Enter a year: ");
						int month = readInt(sc, "Enter a month: ");
						System.out.println();
						query1(con, month, year);
						break;
					case 2:
						int player_id = readInt(sc, "Enter the player id: ");
						int team_id = readInt(sc, "Enter the team id: ");
						System.out.println();
						query2(con, player_id, team_id);
						break;
					case 3:
						System.out.print("Enter a nationality: ");
						String nationality = sc.nextLine();
						query3(con, nationality, readInt(sc, "Enter a year: "));
						break;
					case 4:
						query4(con);
						break;
					case 5:
						query5(con);
						break;
					case 6:
						query6(con);
						break;
					case 7:
						query7(con);
						break;
					case 8:
						return;
					default:
						System.out.println("Entry has to be between 1 and 7");
				}
				con.commit();
				System.out.println();
				System.out.print("Press ENTER to continue:");
				sc.nextLine();
				System.out.println();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		update(con, "DROP DATABASE " + jdbcDatabase);
	}
}
