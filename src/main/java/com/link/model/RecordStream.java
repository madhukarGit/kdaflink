package com.link.model;

import lombok.*;

import java.util.List;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
public class RecordStream {

    private String recordId;
    private String recordName;
    private String recordData;

    @Override
    public String toString() {
        return "Record to string {" +
                "recordId='" + recordId + '\'' +
                ", recordName='" + recordName + '\'' +
                ", recordData=" + recordData +
                '}';
    }
}
