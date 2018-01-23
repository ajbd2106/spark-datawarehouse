package com.postgresql;

import lombok.Getter;

@Getter
public class PostGreSqlConfig {

    private final String driver = "org.postgresql.Driver";
    private final String url = "jdbc:postgresql://localhost:5432/postgres";
    private final String user = "marc";
    private final String password = "TP1012!DC";

    private final String urlWithCredentials = String.format("%s?user=%s&password=%s", url, user, password);

    private PostGreSqlConfig() {
    }


    private static PostGreSqlConfig INSTANCE = new PostGreSqlConfig();

    public static PostGreSqlConfig getInstance() {
        return INSTANCE;
    }
}
