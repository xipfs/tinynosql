package net.xipfs.tinynosql.core.sstable.blocks;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class Descriptor {
	public final String base, ns, cf;
	public final String[] columns;
	public final Set<String> columnSet;

	public Descriptor(String base, String ns, String cf, String[] columns) {
		this.base = base;
		this.ns = ns;
		this.cf = cf;
		this.columns = columns;
		columnSet = new TreeSet<>(Arrays.asList(columns));
	}

	public boolean hasColumn(String column) {
		return columnSet.contains(column);
	}

	public File getDir() {
		File dir = new File(new File(base, ns), cf);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return dir;
	}
}
