package net.xipfs.tinynosql.test.io.sstable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import org.junit.Test;

import net.xipfs.tinynosql.core.sstable.SSTable;
import net.xipfs.tinynosql.core.sstable.SSTableConfig;
import net.xipfs.tinynosql.core.sstable.blocks.Descriptor;


public class SSTableTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col1", "col2"});
    SSTableConfig config = SSTableConfig.builder()
            .setMemTablesLimit(4)
            .build();

    SSTable sst = new SSTable(desc, "col1", config);

    @Test
    public void get() throws Exception {
        System.out.println(new Date());
        for (int i = 0; i < Short.MAX_VALUE; i++) {
            String row = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            //String val = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            Optional<String> v = sst.get(row);
            if (i % 2 == 0) {
                assertTrue(v.isPresent());
                System.out.println("put: " + row + ", " + v.get());
            } else {
                assertFalse(v.isPresent());
                System.out.println("delete: " + row);
            }
        }
        System.out.println(new Date());
    }

    //    @Ignore
    @Test
    public void put() throws Exception {
        Instant start = Instant.now();
        System.out.println(start);
        for (int i = 0; i < 2000000; i++) {
            String row = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            String val = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            if (i % 2 == 0) {
                sst.put(row, val);
            } else {
                sst.put(row, null);
            }
        }
        System.out.println("2000000 put used: " + Duration.between(start, Instant.now()));

        start = Instant.now();
        for (int i = 2000000 - 1; i >= 0; i--) {
            String row = "test " + i + "qwertyuiopasdfghjklzxcvbnm";
            Optional<String> v = sst.get(row);
            if (i % 2 == 0) {
                assertTrue(v.isPresent());
                System.out.println("put: " + row + ", " + v.get());
            } else {
                assertFalse(v.isPresent());
                System.out.println("delete: " + row);
            }
        }
        System.out.println("2000000 get used: " + Duration.between(start, Instant.now()));
    }

}