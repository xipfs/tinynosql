package net.xipfs.tinynosql.core.interfaces;

public interface Filter {
    boolean isPresent(String key);
    long[] toLongs();
}
