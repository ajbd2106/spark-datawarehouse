package com.sink.postgres.command;

public class RenameTableCommand implements PostGreCommand {

    private final String currentName;
    private final String newName;

    public RenameTableCommand(String currentName, String newName) {
        this.currentName = currentName;
        this.newName = newName;
    }

    @Override
    public String getCommand() {
        return String.format("ALTER TABLE %s RENAME TO %s", currentName, newName);
    }

    @Override
    public String getErrorMessage() {
        return String.format("Error while renaming table %s to %s", currentName, newName);
    }
}
