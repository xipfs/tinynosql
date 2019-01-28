package net.xipfs.tinynosql.core;


public final class NoSQLInterface {
    private NoSQLInterface() {}

    /**
     * 创建数据库实例
     *
     * @param applicationName 实例名称
     * @return 数据库实例
     */
    public static Application createApplication(String applicationName) {
        return new Application(applicationName);
    }

    /**
     * 创建表
     *
     * @param tableName   表名
     * @param columnNames 列明
     * @return 表
     */
    public static Table createTable(String tableName, String[] columnNames) {
        return new Table(tableName, columnNames);
    }
}
