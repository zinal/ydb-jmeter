package tech.ydb.jmeter;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author mzinal
 */
public class YdbQueryResult {
    
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(YdbQueryResult.class);

    private final byte[] data;
    private final int retryCount;

    public YdbQueryResult(byte[] data, int retryCount) {
        this.data = data;
        this.retryCount = retryCount;
    }

    public byte[] getData() {
        return data;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public URL makeURL() {
        try {
            return new URL("http://ydb-query/info?retryCount=" + String.valueOf(retryCount));
        } catch(MalformedURLException mue) {
            LOGGER.warn("Failed to report URL-formatted data", mue);
        }
        return null;
    }

}
