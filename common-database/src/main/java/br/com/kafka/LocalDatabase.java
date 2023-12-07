package br.com.kafka;

import com.google.gson.Gson;

import java.sql.*;

public class LocalDatabase {
    private final Connection connection;

    Gson gson = new Gson();

    public LocalDatabase(final String name) throws SQLException {
        String url = "jdbc:sqlite:target/"+name+".db";
        connection = DriverManager.getConnection(url);
    }

    // way generic, sql inject is possible, but it is just for tests
    public void createIfNotExists(final String sql){
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String ... params) throws SQLException {
        return getPreparedStatement(statement, params).execute();
    }

    public ResultSet query(String query, String ... params) throws SQLException {
        return getPreparedStatement(query, params).executeQuery();
    }
    private PreparedStatement getPreparedStatement(String statement, String[] params) throws SQLException {
        var preparedStatment = connection.prepareStatement(statement);
        for(int i = 0; i < params.length; i++) {
            preparedStatment.setString(i+1, params[i]);
        }
        return preparedStatment;
    }
}
