import java.sql.*;
public class Sql {
    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection(a[0], "payflow", "payflow"); Statement s = c.createStatement()) {
            c.setAutoCommit(true);
            for (int i = 1; i < a.length; i++) {
                if (s.execute(a[i])) {
                    ResultSet r = s.getResultSet(); int n = r.getMetaData().getColumnCount();
                    while (r.next()) { StringBuilder b = new StringBuilder(); for (int j = 1; j <= n; j++) b.append(r.getString(j)).append(j < n ? " | " : ""); System.out.println(b); }
                } else System.out.println("ok " + s.getUpdateCount());
            }
        }
    }
}
