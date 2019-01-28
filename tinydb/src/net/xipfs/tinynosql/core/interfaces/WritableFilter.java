package net.xipfs.tinynosql.core.interfaces;

public interface WritableFilter extends Filter {
    void add(String key);
}
