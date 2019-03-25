import com.sun.istack.internal.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Stream;

public class SQLWrapper {
	private Connection con;

	public void commit() throws SQLException {
		//con.commit();
	}

	public void query(String query) throws SQLException {
		PreparedStatement ps = con.prepareStatement(query);
		try {
			ps.execute();
		}
		catch (SQLException e) {
			throw new SQLException("Bad query \"" + query + "\"\n" + e.getMessage());
		}
	}

	public void insertCsv(String csvFname, boolean ignoreFirstLine, String tableName, String any_field, @Nullable String values) throws FileNotFoundException, IOException, SQLException {
		if (values == null) {
			values = "";
		}

		SimpleFileReader sfr = new SimpleFileReader(csvFname);

		Stream<String> fileStream = sfr.stream();
		if (ignoreFirstLine) {
			fileStream = fileStream.skip(1);
		}
		StringBuilder sb = new StringBuilder();
		fileStream.forEach(l -> {
			String[] arr = (l + " ").split(",");
			for (int i = 0; i < arr.length; ++i) {
				arr[i] = arr[i].trim();
				if (arr[i].equals("")) {
					arr[i] = "NULL";
				}
			}
			sb.append("(");
			sb.append(Stream.of(arr).reduce((a, v) -> a + "," + v).orElseThrow(() -> new RuntimeException("Reduce is fukt")));
			sb.append("),");
		});

		sb.deleteCharAt(sb.length() - 1);
		query("INSERT INTO " + tableName + " " + values + " VALUES " + sb.toString() + " ON DUPLICATE KEY UPDATE " + any_field + "=" + any_field + ";");
	}

	public SQLWrapper(String driver, String url, String user, String password, String database) throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		System.setProperty("jdbc.drivers", driver);

		con = DriverManager.getConnection(url, user, password);
		//con.setAutoCommit(false);

		query("CREATE DATABASE IF NOT EXISTS " + database + ";");
		con.setCatalog(database);
	}
}
