package tech.ydb.jmeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

/**
 * As pre- and post-processors essentially do the same, so this class provides the implementation.
 *
 * @author zinal
 */
public abstract class AbstractYdbProcessor extends AbstractYdbTestElement {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractYdbTestElement.class);

    /**
     * Calls the YDB code to be executed.
     */
    protected void process() {
        if (StringUtils.isBlank(getDataSource())) {
            throw new IllegalArgumentException("Name for DataSoure must not be empty in " + getName());
        }
        try {
            YdbConnection conn = YdbConfigElement.getConnection(getDataSource());
            execute(conn);
        } catch(Exception ex) {
            LOG.error("Processing failed on {}", getName(), ex);
        }
    }

}
