public class Main {
    private final static String dbName = "PROJ3";

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        try {
            SQLWrapper s = new SQLWrapper("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306", "root", "toor", "proj3");

            s.query("CREATE TABLE IF NOT EXISTS players (" +
                    "player_id INT NOT NULL, " +
                    "tag VARCHAR(255) NOT NULL, " +
                    "real_name VARCHAR(255) NOT NULL, " +
                    "nationality CHAR(2) NOT NULL, " +
                    "birthday DATE NOT NULL, " +
                    "race CHAR(1) NOT NULL, " +
                    "PRIMARY KEY (player_id));");
            s.insertCsv("players.csv", true, "players", "player_id", null);

            s.query("CREATE TABLE IF NOT EXISTS teams (" +
                    "team_id INT NOT NULL, " +
                    "name VARCHAR(255) NOT NULL, " +
                    "founded DATE NOT NULL, " +
                    "disbanded DATE, " +
                    "PRIMARY KEY (team_id));");
            s.insertCsv("teams.csv", true, "teams", "team_id", null);

            s.query("CREATE TABLE IF NOT EXISTS members (" +
                    "player INT NOT NULL, " +
                    "team INT NOT NULL, " +
                    "start_date DATE NOT NULL, " +
                    "end_date DATE, " +
                    "FOREIGN KEY (player) REFERENCES players (player_id), " +
                    "FOREIGN KEY (team) REFERENCES teams (team_id));");
            s.insertCsv("members.csv", true, "members", "player", null);

            s.query("CREATE TABLE IF NOT EXISTS tournaments (" +
                    "tournament_id INT NOT NULL, " +
                    "name VARCHAR(255) NOT NULL, " +
                    "REGION CHAR(2), " +
                    "major BOOLEAN NOT NULL, " +
                    "PRIMARY KEY (tournament_id));");
            s.insertCsv("tournaments.csv", true, "tournaments", "tournament_id", null);

            s.query("CREATE TABLE IF NOT EXISTS matches (" +
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
            s.insertCsv("matches_v2.csv", true, "matches", "match_id", null);

            s.query("CREATE TABLE IF NOT EXISTS earnings " +
                    "(tournament INT NOT NULL, " +
                    "player INT NOT NULL, " +
                    "prize_money INT NOT NULL, " +
                    "position INT NOT NULL, " +
                    "PRIMARY KEY (position), " +
                    "FOREIGN KEY (tournament) REFERENCES tournaments (tournament_id), " +
                    "FOREIGN KEY (player) REFERENCES players (player_id));");
            s.insertCsv("earnings.csv", true, "earnings", "tournament", null);


            s.commit();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
