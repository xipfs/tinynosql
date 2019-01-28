package net.xipfs.tinynosql.core;

import net.xipfs.tinynosql.core.sstable.SSTable;
import net.xipfs.tinynosql.core.sstable.SSTableConfig;
import net.xipfs.tinynosql.core.sstable.blocks.Descriptor;
import net.xipfs.tinynosql.utils.Qualifier;
import net.xipfs.tinynosql.utils.Row;
import net.xipfs.tinynosql.utils.Timed;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.*;


public class Table implements Flushable, Closeable {
	// 表名
    private String tableName;
    // 表配置
    private SSTableConfig config;
    // SSTable 集合，Column与SSTable  1：1
    private Map<String, SSTable> sstMap;
    // 缓存最近访问的行
    private PriorityQueue<Timed<Row>> recentlyAccessedRows;

    public Table(String tableName, String[] columnNames) {
        this.tableName = tableName;
        this.sstMap = new HashMap<>();
        this.config = SSTableConfig.defaultConfig();
        this.recentlyAccessedRows = new PriorityQueue<Timed<Row>>(config.getRowCacheCapacity());
        Descriptor desc = new Descriptor(tableName, "", "", columnNames);
        for (String columnName : columnNames) {
            this.sstMap.put(columnName, new SSTable(desc, columnName, config));
        }
    }

    /**
     * 插入数据
     * @param row
     * @throws IOException
     */
    public void insert(Row row) throws IOException {
    	// 把数据带上时间戳保存到缓存
        this.recentlyAccessedRows.add(new Timed<>(row));
        // 同时放入 SSTable
        for (String columnName : this.sstMap.keySet()) {
            if (row.hasColumn(columnName)) {
                this.sstMap.get(columnName).put(row.getRowKey(), row.getColumnValue(columnName));
            }
        }
    }
    /**
     *  根据 rowKey 查找数据
     * @param rowKey
     * @return
     * @throws InterruptedException
     */
    public Row selectRowKey(String rowKey) throws InterruptedException {
    	// 先从缓存中查找数据
        Iterator<Timed<Row>> itr = this.recentlyAccessedRows.iterator();
        while (itr.hasNext()) {
            Row rowToCheck = itr.next().get();
            if (rowToCheck.getRowKey().equals(rowKey)) {
                return rowToCheck;
            }
        }
        // 从 SSTable 中查找
        Map<String, String> columnValues = new HashMap<>();
        for (String columnName : this.sstMap.keySet()) {
            columnValues.put(columnName, this.sstMap.get(columnName).get(rowKey).orElse(null));
        }
        for (String columnValue : columnValues.values()) {
            if (columnValue != null) {
                Row newRow = new Row(rowKey, columnValues);
                this.recentlyAccessedRows.add(new Timed<>(newRow));
                return newRow;
            }
        }
        return null;
    }

    /**
     * 查找列名为 columnName 值为 columnValue 的所有行
     * @param columnName
     * @param columnValue
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public List<Row> selectRowWithColumnValue(String columnName, String columnValue)
            throws IOException, InterruptedException {
        Qualifier q = new Qualifier("=", columnValue);
        List<Row> result = this.selectRowsWithQualifier(columnName, q);
        for (Row row : result) {
            this.recentlyAccessedRows.add(new Timed<>(row));
        }
        return result;
    }

    public List<Row> selectRowsWithColumnRange(String columnName, String operator, String target) throws IOException, InterruptedException {
        Qualifier q = new Qualifier(operator, target);
        return this.selectRowsWithQualifier(columnName, q);
    }

    private List<Row> selectRowsWithQualifier(String columnName, Qualifier q) throws IOException, InterruptedException {
        Map<String, Row> result = new HashMap<>();
        Map<String, String> selected = this.sstMap.get(columnName).getColumnWithQualifier(q);
        for (Map.Entry<String, String> entry : selected.entrySet()) {
            String rowKey = entry.getKey();
            String colValue = entry.getValue();
            Row toAdd = new Row(rowKey, new HashMap<>());
            toAdd.addColumn(columnName, colValue);
            result.put(rowKey, toAdd);
        }

        Qualifier rowQ = new Qualifier(result.keySet());
        for (String colName : this.sstMap.keySet()) {
            if (!colName.equals(columnName)) {
                Map<String, String> colValues = this.sstMap.get(colName).getColumnWithQualifier(rowQ);
                for (Map.Entry<String, String> entry : colValues.entrySet()) {
                    String rowKey = entry.getKey();
                    String colValue = entry.getValue();
                    if (result.containsKey(rowKey)) {
                        result.get(rowKey).addColumn(colName, colValue);
                    }
                }
            }
        }
  
        for (Row row : result.values()) {
            recentlyAccessedRows.add(new Timed<>(row));
        }

        return new ArrayList<>(result.values());
    }


    public void insertColumnValues(String columnName, Map<String, String> rowKeyAndValues) throws IOException {
        for (Map.Entry<String, String> entry : rowKeyAndValues.entrySet()) {
            this.sstMap.get(columnName).put(entry.getKey(), entry.getValue());
        }
    }

    public void deleteRowKey(String rowKey) throws IOException {
        for (SSTable table : this.sstMap.values()) {
            table.put(rowKey, null);
        }
    }

    public String getTableName() {
        return this.tableName;
    }

    public void update(Row row) throws IOException {
        this.insert(row);
    }

    @Override
    public void flush() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.flush();
        }
    }

    @Override
    public void close() throws IOException {
        for (SSTable t : sstMap.values()) {
            t.close();
        }
        sstMap.clear();
    }
}
