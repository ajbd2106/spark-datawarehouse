package com.csv;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

@Getter
public class CsvStream {
    private final ByteArrayInputStream content;

    public CsvStream(String content) {
        this.content = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
