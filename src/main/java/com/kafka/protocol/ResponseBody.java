package com.kafka.protocol;

import com.kafka.protocol.io.DataOutput;

public interface ResponseBody {

    void serialize(DataOutput output);

}
