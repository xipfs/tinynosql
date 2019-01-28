package net.xipfs.tinynosql.core.interfaces;


public interface Hasher<T> {
    long hash(T t);
}
