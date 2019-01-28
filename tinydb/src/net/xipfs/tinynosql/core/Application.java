package net.xipfs.tinynosql.core;

import java.util.HashMap;
import java.util.Map;

public class Application {
	//实例名称
    private final String applicationName;
    //表集合
    private Map<String, Table> tableMap;

    public Application(String applicationName) {
        this.applicationName = applicationName;
        this.tableMap = new HashMap<>();
    }

    public String getApplicationName() {
        return this.applicationName;
    }

    public boolean addTable(Table table) {
        if (!this.tableMap.containsKey(table.getTableName())) {
            this.tableMap.put(table.getTableName(), table);
        }
        return false;
    }

    public Table getTable(String tableName) {
        return this.tableMap.getOrDefault(tableName, null);
    }
}
