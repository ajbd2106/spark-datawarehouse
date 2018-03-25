package com.sink.postgres.command;

public class TruncateTableCommand implements PostGreCommand {

    private final String tableName;

    public TruncateTableCommand(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getCommand() {
        return String.format("TRUNCATE TABLE %s", tableName);
    }

    @Override
    public String getErrorMessage() {
        return String.format("could not truncate table %s", tableName);
    }
}
