package tech.ydb.jmeter;

import org.apache.jmeter.gui.TestElementMetadata;
import org.apache.jmeter.processor.PostProcessor;
import org.apache.jmeter.testbeans.TestBean;

/**
 * Post processor handling YDB Requests
 *
 * @author zinal
 */
@TestElementMetadata(labelResource = "displayName")
public class YdbPostProcessor extends AbstractYdbProcessor implements TestBean, PostProcessor {

    private static final long serialVersionUID = 1L;

    @Override
    public void process() {
        super.process();
    }

}
