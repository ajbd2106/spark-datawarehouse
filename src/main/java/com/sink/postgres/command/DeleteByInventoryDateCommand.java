package com.sink.postgres.command;

public class DeleteByInventoryDateCommand implements PostGreCommand {

    private final String tableName;
    private final String inventoryDateFieldName;
    private final Integer inventoryDate;

    public DeleteByInventoryDateCommand(String tableName, String inventoryDateFieldName, Integer inventoryDate) {
        this.tableName = tableName;
        this.inventoryDateFieldName = inventoryDateFieldName;
        this.inventoryDate = inventoryDate;
    }

    @Override
    public String getCommand() {
        return String.format("DELETE FROM %s WHERE %s = %d", tableName, inventoryDateFieldName, inventoryDate);
    }

    @Override
    public String getErrorMessage() {
        return String.format("Error while deleting table %s with inventory field name %s and inventory date %s", tableName, inventoryDateFieldName, String.valueOf(inventoryDate));
    }
}
