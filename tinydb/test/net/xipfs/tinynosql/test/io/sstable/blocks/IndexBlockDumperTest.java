package net.xipfs.tinynosql.test.io.sstable.blocks;

import net.xipfs.tinynosql.core.sstable.SSTableConfig;
import net.xipfs.tinynosql.core.sstable.blocks.Descriptor;
import net.xipfs.tinynosql.core.sstable.blocks.IndexBlock;
import net.xipfs.tinynosql.core.sstable.blocks.IndexBlockDumper;
import net.xipfs.tinynosql.core.sstable.blocks.IndexBlockLoader;
import net.xipfs.tinynosql.utils.Pair;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;


public class IndexBlockDumperTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void dump() throws Exception {
        IndexBlock idx = new IndexBlock(desc, "col", 0, config);
        IndexBlockDumper idxDumper = new IndexBlockDumper(idx);
        ArrayList<Pair<String, String>> ranges = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String r1 = RandomStringUtils.randomAlphabetic(10);
            String r2 = RandomStringUtils.randomAlphabetic(10);
            if (r1.compareTo(r2) < 0)
                ranges.add(new Pair<>(r1, r2));
            else ranges.add(new Pair<>(r2, r1));
        }
        idxDumper.dump(ranges);
        IndexBlockLoader idxLoader = new IndexBlockLoader(idx);
        ArrayList<Pair<String, String>> ranges2 = idxLoader.getRanges();
        for (Pair<String, String> r : ranges2) {
            System.out.println(r);
        }
        assertArrayEquals(ranges.toArray(), ranges2.toArray());
    }

}