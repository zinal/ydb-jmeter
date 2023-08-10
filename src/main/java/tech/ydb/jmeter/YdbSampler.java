package tech.ydb.jmeter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.ObjectUtils;

import org.apache.commons.lang3.StringUtils;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.engine.util.ConfigMergabilityIndicator;
import org.apache.jmeter.gui.TestElementMetadata;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.Sampler;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestElement;

/**
 * A sampler which understands YDB database requests.
 *
 * @author zinal
 */
@TestElementMetadata(labelResource = "displayName")
public class YdbSampler extends AbstractYdbTestElement
        implements Sampler, TestBean, ConfigMergabilityIndicator {

    private static final long serialVersionUID = 1L;

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<>(
            Arrays.asList("org.apache.jmeter.config.gui.SimpleConfigGui"));

    public YdbSampler() {
    }

    @Override
    public SampleResult sample(Entry e) {
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        res.setSamplerData(toString());
        res.setDataType(SampleResult.TEXT);
        res.setContentType("text/plain");
        res.setDataEncoding(CHARSET.name());

        // Assume we will be successful
        res.setSuccessful(true);
        res.setResponseMessageOK();
        res.setResponseCodeOK();

        res.sampleStart();

        try {
            String dataSource = getDataSource();
            if (StringUtils.isBlank(dataSource)) {
                throw new IllegalArgumentException("Name for DataSoure must not be empty in " + getName());
            }

            YdbConnection conn;
            try {
                conn = YdbConfigElement.getConnection(dataSource);
            } finally {
                res.connectEnd();
            }
            YdbQueryResult result = execute(conn.getRetryCtx(), res);
            res.setResponseData(result.getData());
            res.setResponseHeaders(YdbConfigElement.getConnectionInfo(dataSource));
            res.setURL(result.makeURL());
        } catch (Exception ex) {
            res.setResponseMessage(YdbUtils.fullMessage(ex));
            res.setResponseCode("000");
            res.setResponseData(
                    ObjectUtils.defaultIfNull(ex.getMessage(), "NO MESSAGE"),
                    res.getDataEncodingWithDefault());
            res.setSuccessful(false);
        }

        // TODO: process warnings? Set Code and Message to success?
        res.sampleEnd();
        return res;
    }

    /**
     * @see org.apache.jmeter.samplers.AbstractSampler#applies(org.apache.jmeter.config.ConfigTestElement)
     */
    @Override
    public boolean applies(ConfigTestElement configElement) {
        String guiClass = configElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }
}
