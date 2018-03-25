package com.sink.postgres.command;

public class DropTableCommand implements PostGreCommand {

    private final String tableName;

    public DropTableCommand(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getCommand() {
        return String.format("DROP TABLE IF EXISTS %s", tableName);
    }

    @Override
    public String getErrorMessage() {
        return String.format("Error while dropping table %s", tableName);
    }
}
