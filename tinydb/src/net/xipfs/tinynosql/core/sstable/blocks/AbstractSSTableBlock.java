package net.xipfs.tinynosql.core.sstable.blocks;

import java.util.NoSuchElementException;

import net.xipfs.tinynosql.utils.Modification;

public abstract class AbstractSSTableBlock {

    public abstract Modification get(String row) throws NoSuchElementException;
}
