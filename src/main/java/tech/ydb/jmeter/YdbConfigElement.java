package tech.ydb.jmeter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import org.apache.jmeter.config.ConfigElement;
import org.apache.jmeter.gui.TestElementMetadata;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testbeans.TestBeanHelper;
import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

/**
 * YDB connection configuration element.
 *
 * @author zinal
 */
@TestElementMetadata(labelResource = "displayName")
public class YdbConfigElement extends AbstractTestElement
        implements ConfigElement, TestStateListener, TestBean {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(YdbConfigElement.class);

    private transient String dataSource;
    private transient String endpoint;
    private transient String database;
    private transient String authMode;
    private transient String username;
    private transient String password;
    private transient String saKeyFile;
    private transient String tlsCertFile;
    private transient String poolMax;
    private transient String connectionAge;
    private transient String timeout;

    public static String getConnectionInfo(String poolName) {
        Object poolObject =
                JMeterContextService.getContext().getVariables().getObject(poolName);
        if (poolObject instanceof YdbConnection) {
            YdbConnection pool = (YdbConnection) poolObject;
            return pool.getConnectionInfo();
        } else {
            return "Object:" + poolName + " is not of expected type '" + YdbConnection.class.getName() + "'";
        }
    }

    public static YdbConnection getConnection(String poolName) {
        Object poolObject =
                JMeterContextService.getContext().getVariables().getObject(poolName);
        if (poolObject == null) {
            throw new RuntimeException("No pool found named: '" + poolName
                    + "', ensure Variable Name matches Variable Name of YDB Connection Configuration");
        } else {
            if(poolObject instanceof YdbConnection) {
                YdbConnection pool = (YdbConnection) poolObject;
                return pool;
            } else {
                String errorMsg = "Found object stored under variable:'" + poolName + "' with class:"
                        + poolObject.getClass().getName() + " and value: '" + poolObject
                        + " but it's not a DataSourceComponent, check you're not already using this name as another variable";
                LOG.error("{}", errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }

    @Override
    public void addConfigElement(ConfigElement config) {
    }

    @Override
    public boolean expectsModification() {
        return false;
    }

    @Override
    public void testStarted() {
        synchronized(this) {
            this.setRunningVersion(true);
            TestBeanHelper.prepare(this);
            JMeterVariables variables = getThreadContext().getVariables();
            String poolName = getDataSource();
            if (StringUtils.isBlank(poolName)) {
                throw new IllegalArgumentException("Name for DataSoure must not be empty in " + getName());
            } else if (variables.getObject(poolName) != null) {
                LOG.error("YDB data source already defined for: {}", poolName);
            } else {
                variables.putObject(poolName, new YdbConnection(this));
            }
        }
    }

    @Override
    public void testStarted(String host) {
        testStarted();
    }

    @Override
    public void testEnded() {
        synchronized(this) {
            JMeterVariables variables = getThreadContext().getVariables();
            String poolName = getDataSource();
            if (! StringUtils.isBlank(poolName)) {
                Object o = variables.getObject(poolName);
                if (o instanceof YdbConnection) {
                    ((YdbConnection)o).close();
                    variables.remove(poolName);
                }
            }
        }
    }

    @Override
    public void testEnded(String host) {
        testEnded();
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public AuthMode getAuthModeCode() {
        return AuthMode.valueOf(authMode.toUpperCase());
    }

    public String getAuthMode() {
        return authMode;
    }

    public void setAuthMode(String authMode) {
        this.authMode = authMode;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTlsCertFile() {
        return tlsCertFile;
    }

    public void setTlsCertFile(String tlsCertFile) {
        this.tlsCertFile = tlsCertFile;
    }

    public String getSaKeyFile() {
        return saKeyFile;
    }

    public void setSaKeyFile(String saKeyFile) {
        this.saKeyFile = saKeyFile;
    }

    public int getPoolMaxInt() {
        String v = getPoolMax();
        int vx = -1;
        if (v!=null && v.trim().length() > 0)
            vx = Integer.parseInt(v);
        if (vx > 0) {
            return vx;
        }
        return 1 + 2 * Runtime.getRuntime().availableProcessors();
    }

    public String getPoolMax() {
        return poolMax;
    }

    public void setPoolMax(String poolMax) {
        this.poolMax = poolMax;
    }

    public String getConnectionAge() {
        return connectionAge;
    }

    public void setConnectionAge(String connectionAge) {
        this.connectionAge = connectionAge;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    public enum AuthMode {

        NONE,
        ENV,
        METADATA,
        SAKEY,
        STATIC

    }
}
