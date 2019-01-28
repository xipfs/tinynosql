package net.xipfs.tinynosql.core.interfaces;

import java.io.File;
import java.io.IOException;


public interface Block {
    File getFile() throws IOException;
}
