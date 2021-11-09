package com.github.ylgrgyq.reservoir.storage2;

import javax.annotation.Nullable;
import java.io.IOException;

public interface LogInputStream<T extends RecordBatch> {
    @Nullable
    T nextBatch() throws IOException;
}
