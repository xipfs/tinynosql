package net.xipfs.tinynosql.core.sstable.blocks;

import net.xipfs.tinynosql.core.interfaces.Filter;
import net.xipfs.tinynosql.utils.Modification;
import net.xipfs.tinynosql.utils.Modifications;

import org.apache.commons.compress.compressors.CompressorException;

import java.io.IOException;

public class DataBlockDumper {
	private final TempDataBlock tmpDataBlock;
	private final int filterBits;
	private final boolean compressible;

	public DataBlockDumper(TempDataBlock tmpDataBlock, int filterBits, boolean compressible) {
		this.tmpDataBlock = tmpDataBlock;
		this.filterBits = filterBits;
		this.compressible = compressible;
	}

	public DataBlockDumper(TempDataBlock tmpDataBlock, int filterBits) {
		this(tmpDataBlock, filterBits, false);
	}

	/**
	 * Dumps the modifications into the current temporary block. The number of
	 * longs in the filter should match the filterBits.
	 *
	 * @param modifications
	 *            modifications to dump
	 * @param filter
	 *            the bloom filter to dump in the file
	 * @throws IOException
	 */
	public void dump(Modifications modifications, Filter filter) throws IOException {
		tmpDataBlock.requireFileExists();
		ComponentFile c = null;
		try {
			c = compressible ? tmpDataBlock.getCompressibleWritableComponentFile()
					: tmpDataBlock.getWritableComponentFile();
			long[] longs = filter.toLongs();
			if (longs.length * Long.SIZE != filterBits)
				throw new IOException("filter length mismatch");
			c.writeFilter(filter);
			for (String row : modifications.rows()) {
				// System.out.println(row);
				c.writeString(row);
				Modification mod = modifications.get(row);
				if (mod.isPut()) {
					c.writeString(mod.getIfPresent().get());
				} else {
					c.writeString("");
				}
				c.writeLong(mod.getTimestamp());
			}
		} catch (CompressorException ce) {
			throw new IOException(ce.getMessage());
		} finally {
			ComponentFile.tryClose(c);
		}
	}
}
