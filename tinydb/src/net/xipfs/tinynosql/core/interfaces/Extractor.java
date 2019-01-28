package net.xipfs.tinynosql.core.interfaces;

import java.io.IOException;

public interface Extractor<T> {
    T extract() throws IOException;
}
