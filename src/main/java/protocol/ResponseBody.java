package protocol;

import protocol.io.DataOutput;

public interface ResponseBody {

    void serialize(DataOutput output);

}
