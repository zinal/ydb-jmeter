package tech.ydb.jmeter;

import java.net.URL;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author mzinal
 */
public class YdbQueryResultTest {

    @Test
    public void test1() {
        URL url = new YdbQueryResult(new byte[0], 10).makeURL();
        String expected = "http://ydb-query/info?retryCount=10";
        String actual = url.toString();
        Assert.assertEquals(expected, actual);
    }

}
