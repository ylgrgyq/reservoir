package com.github.ylgrgyq.reservoir.storage2;

import javax.annotation.Nullable;

enum FileType {
    Unknown(""),
    Lock("LOCK"),
    Segment("SEGMENT", ".log");

    static FileType parseFileType(String fileName) {
        for (FileType type : FileType.values()) {
            if (type != Unknown && fileName.startsWith(type.prefix)) {
                return type;
            }
        }
        return Unknown;
    }

    private String prefix;
    @Nullable
    private String suffix;

    FileType(String prefix) {
        this(prefix, null);
    }

    FileType(String prefix, @Nullable String suffix) {
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public String prefix() {
        return prefix;
    }

    @Nullable
    public String suffix() {
        return suffix;
    }

    boolean match(String fileName) {
        if (suffix() != null) {
            return fileName.startsWith(prefix()) && fileName.endsWith(suffix());
        } else {
            return fileName.startsWith(prefix());
        }
    }
}
