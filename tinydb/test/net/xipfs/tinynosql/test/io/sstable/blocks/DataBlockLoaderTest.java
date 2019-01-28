package net.xipfs.tinynosql.test.io.sstable.blocks;

import net.xipfs.tinynosql.core.sstable.SSTableConfig;
import net.xipfs.tinynosql.core.sstable.blocks.DataBlock;
import net.xipfs.tinynosql.core.sstable.blocks.DataBlockLoader;
import net.xipfs.tinynosql.core.sstable.blocks.Descriptor;
import net.xipfs.tinynosql.utils.Modification;
import net.xipfs.tinynosql.utils.Modifications;

import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class DataBlockLoaderTest {
    Descriptor desc = new Descriptor("base", "ns", "cf", new String[]{"col"});
    SSTableConfig config = SSTableConfig.defaultConfig();

    @Test
    public void get() throws Exception {
        DataBlock block = new DataBlock(desc, "col", 0, 3, config);
        DataBlockLoader loader = new DataBlockLoader(
                block, config.getPerBlockBloomFilterBits(), config.getHasher());
        Modifications mod = loader.extractModifications(config.getBlockBytesLimit());
        for (String row : mod.rows()) {
            Modification fetch = loader.get(row);
            if (mod.get(row).isPut()) {
                System.out.println(fetch.getIfPresent().get() + " = " + mod.get(row).getIfPresent().get());
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            } else {
                System.out.println("delete");
                System.out.println(fetch.getTimestamp() + " = " + mod.get(row).getTimestamp());
            }
            assertTrue(fetch.equals(mod.get(row)));
        }
    }

}