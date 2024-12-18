package tech.ydb.jmeter;

/**
 *
 * @author mzinal
 */
public class YdbQueryResult {
    
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(YdbQueryResult.class);

    private final byte[] data;

    public YdbQueryResult() {
        this.data = new byte[0];
    }

    public YdbQueryResult(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

}
