package com.link.schema;

import com.link.model.RecordStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class RecordSchema implements DeserializationSchema<RecordStream>{

    @Override
    public RecordStream deserialize(byte[] bytes) throws IOException {
        //{"recordId":"6ac7c02c-d969-48cc-9e65-bb1a727e7774","recordName":"kinesis-record","recordData":["flu","alga","chin","kruel"]}
        RecordStream recordStream = null;
        String data = new String(bytes);
        String EMPLOYEE_REGEX = "\\{\"recordId\":(?<recordId>.*)?,\"recordName\":(?<recordName>.*)?,\"recordData\":(?<recordData>.*?\\])";
        Pattern PEOPLE_PAT = Pattern.compile(EMPLOYEE_REGEX , Pattern.COMMENTS | Pattern.DOTALL);
        Matcher matcher = PEOPLE_PAT.matcher(data);
        if(matcher.find()){
            recordStream = RecordStream.builder().recordId(matcher.group("recordId"))
                    .recordName(matcher.group("recordName"))
                    .recordData(matcher.group("recordData")).build();
        }
        return recordStream;
    }

    @Override
    public boolean isEndOfStream(RecordStream recordStream) {
        return false;
    }

    @Override
    public TypeInformation<RecordStream> getProducedType() {
        return TypeInformation.of(RecordStream.class);
    }
}
