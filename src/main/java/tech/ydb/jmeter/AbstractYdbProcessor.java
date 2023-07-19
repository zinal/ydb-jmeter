package tech.ydb.jmeter;

import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author zinal
 */
public abstract class AbstractYdbProcessor extends AbstractYdbTestElement {

    private static final long serialVersionUID = 1L;

    /**
     * Calls the JDBC code to be executed.
     */
    protected void process() {
        if (StringUtils.isBlank(getDataSource())) {
            throw new IllegalArgumentException("Name for DataSoure must not be empty in " + getName());
        }
        YdbConnection conn = YdbConfigElement.getConnection(getDataSource());
        execute(conn.getRetryCtx());
    }

}
