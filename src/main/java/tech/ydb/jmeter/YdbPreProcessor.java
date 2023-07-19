package tech.ydb.jmeter;

import org.apache.jmeter.gui.TestElementMetadata;
import org.apache.jmeter.processor.PreProcessor;
import org.apache.jmeter.testbeans.TestBean;

/**
 * Preprocessor handling YDB Requests
 *
 * @author zinal
 */
@TestElementMetadata(labelResource = "displayName")
public class YdbPreProcessor extends AbstractYdbProcessor implements TestBean, PreProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public void process() {
        super.process();
    }

}
