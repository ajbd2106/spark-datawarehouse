package com.sink.postgres.command;

public class CloneTableCommand implements PostGreCommand{

    private final String tableName;
    private final String tableNameToClone;

    public CloneTableCommand(String tableName, String tableNameToClone) {
        this.tableName = tableName;
        this.tableNameToClone = tableNameToClone;
    }

    @Override
    public String getCommand() {
        return String.format("CREATE TABLE IF NOT EXISTS %s AS select * from %s where 1 = 2", tableName, tableNameToClone);
    }

    @Override
    public String getErrorMessage() {
        return String.format("could not clone table %s from %s", tableName, tableNameToClone);
    }
}
