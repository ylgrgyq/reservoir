package com.github.ylgrgyq.reservoir.storage2;

enum FileType {
    Unknown(""),
    Lock("LOCK"),
    Segment("SEGMENT");

    static FileType parseFileType(String fileName) {
        for (FileType type : FileType.values()) {
            if (fileName.startsWith(type.prefix) && !type.prefix.isEmpty()) {
                return type;
            }
        }
        return FileType.Unknown;
    }

    private String prefix;

    FileType(String prefix) {
        this.prefix = prefix;
    }
}
