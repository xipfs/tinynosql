package net.xipfs.tinynosql.core.interfaces;

public interface StringHasher extends Hasher<String> {
    long hash(String key);
}
