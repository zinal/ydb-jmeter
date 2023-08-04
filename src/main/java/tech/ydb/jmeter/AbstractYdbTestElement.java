package tech.ydb.jmeter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;

import org.apache.jmeter.testelement.AbstractTestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.save.CSVSaveService;
import org.apache.jmeter.threads.JMeterVariables;

import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.result.ValueReader;
import tech.ydb.table.settings.ExecuteDataQuerySettings;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.settings.ExecuteSchemeQuerySettings;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.Type;
import tech.ydb.table.values.Value;

/**
 * A base class for all YDB test elements handling the basics of an SQL request.
 *
 * @author zinal
 */
public abstract class AbstractYdbTestElement extends AbstractTestElement
        implements TestStateListener {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractYdbTestElement.class);

    protected static final java.nio.charset.Charset CHARSET = StandardCharsets.UTF_8;

    private static final String COMMA = ",";
    private static final char COMMA_CHAR = ',';

    // Maximum allowed capacity to store query results.
    public static final int MAX_STORE_CHARS = 65536;

    // Query types (used to communicate with GUI)
    // N.B. These must not be changed, as they are used in the JMX files
    public static final String DATAQUERY   = "Data Query";
    public static final String SCANQUERY   = "Scan Query";
    public static final String SCHEMEQUERY   = "Scheme Query";
    // Transaction modes
    public static final String SERIALIZABLERW = "Serializable Read/Write";
    public static final String ONLINERO = "Online Read Only";
    public static final String STALERO = "Stale Read Only";
    public static final String SNAPSHOTRO = "Snapshot Read Only";
    // ResultSet store modes
    public static final String RS_STORE_AS_STRING = "Store as String";
    public static final String RS_COUNT_RECORDS = "Count Records";

    private String queryType = DATAQUERY;
    private String txType = SERIALIZABLERW;
    private String query = "";
    private String dataSource = "";
    private String queryArguments = "";
    private String queryArgumentsTypes = "";
    private String variableNames = "";
    private String resultSetHandler = RS_STORE_AS_STRING;
    private String queryTimeout = "";
    private String resultSetMaxRows = "";

    protected AbstractYdbTestElement() {
    }

    /**
     * Execute the test element.
     *
     * @param src a {@link SessionRetryContext}
     * @return the result of the execute command
     */
    protected byte[] execute(SessionRetryContext src) {
        return execute(src, new SampleResult());
    }

    /**
     * Execute the test element.
     * Use the sample given as argument to set time to first byte in the "latency" field of the SampleResult.
     *
     * @param src a {@link SessionRetryContext}
     * @param sample a {@link SampleResult} to save the latency
     * @return the result of the execute command
     */
    protected byte[] execute(SessionRetryContext src, SampleResult sample) {
        LOG.debug("executing ydb: {}", getQuery());
        // Based on query return value, get results
        final String qt = getQueryType();
        if (DATAQUERY.equals(qt)) {
            return executeDataQuery(src, sample);
        }
        if (SCANQUERY.equals(qt)) {
            return executeScanQuery(src, sample);
        }
        if (SCHEMEQUERY.equals(qt)) {
            return executeSchemeQuery(src, sample);
        }
        // User provided incorrect query type
        throw new UnsupportedOperationException("Unexpected YDB query type: " + qt);
    }

    private byte[] executeDataQuery(SessionRetryContext src, SampleResult sample) {
        DataQueryResult dqr = src.supplyResult(
                session -> session.executeDataQuery(getQuery(),
                        makeTxControl(), makeParams(), makeDataQuerySettings()))
                .join().getValue();
        sample.latencyEnd();
        final StringBuilder sb = new StringBuilder();
        final int nrs = dqr.getResultSetCount();
        int varPos = 0;
        for (int irs = 0; irs < nrs; irs++) {
            ResultSetReader rsr = dqr.getResultSet(irs);
            sb.append("** Result set #").append(irs+1)
                    .append(", ").append(rsr.getRowCount()).append(" row(s)");
            if (rsr.isTruncated()) {
                sb.append(" (TRUNCATED)");
            }
            sb.append("\n");
            if (RS_STORE_AS_STRING.equalsIgnoreCase(getResultSetHandler())) {
                appendColumns(sb, rsr);
                appendRows(sb, rsr, 0);
            }
            varPos = storeVariables(rsr, varPos);
        }
        return sb.toString().getBytes(CHARSET);
    }

    private byte[] executeScanQuery(SessionRetryContext src, SampleResult sample) {
        // Input-output data for async operations
        class ScanQueryContext {
            boolean latencyEnd;
            StringBuilder data;
            boolean needHeader;
            boolean storeVariables;
            long totalRows;
        }
        final ScanQueryContext sqc = new ScanQueryContext();
        sqc.latencyEnd = true;
        if ( RS_STORE_AS_STRING.equalsIgnoreCase(getResultSetHandler()) ) {
            sqc.data = new StringBuilder();
            sqc.needHeader = true;
        }
        sqc.storeVariables = true;
        sqc.totalRows = 0;
        src.supplyStatus(session -> {
            GrpcReadStream<ResultSetReader> scan = session.executeScanQuery(getQuery(),
                    makeParams(), makeScanQuerySettings());
            return scan.start(rsr -> {
                if (sqc.latencyEnd) {
                    sample.latencyEnd();
                    sqc.latencyEnd = false;
                }
                if (sqc.data != null) {
                    // Format and store the data rows
                    if (sqc.needHeader) {
                        sqc.needHeader = false;
                        appendColumns(sqc.data, rsr);
                    }
                    appendRows(sqc.data, rsr, sqc.totalRows);
                }
                if (sqc.storeVariables) {
                    sqc.storeVariables = false;
                    storeVariables(rsr, 0);
                }
                sqc.totalRows += rsr.getRowCount();
            });
        }).join().expectSuccess();
        if (sqc.latencyEnd) {
            // Empty result set, need to report latency
            sample.latencyEnd();
        }
        if (sqc.data!=null) {
            sqc.data.append("** Total rows: ").append(sqc.totalRows);
            return sqc.data.toString().getBytes(CHARSET);
        }
        return ("** Total rows: " + Long.toString(sqc.totalRows)).getBytes(CHARSET);
    }

    private byte[] executeSchemeQuery(SessionRetryContext src, SampleResult sample) {
        src.supplyStatus(session -> session.executeSchemeQuery(getQuery(), 
                makeSchemeQuerySettings())).join().expectSuccess();
        sample.latencyEnd();
        return new byte[0];
    }

    private TxControl<?> makeTxControl() {
        if (SERIALIZABLERW.equalsIgnoreCase(txType)) {
            return TxControl.serializableRw();
        }
        if (STALERO.equalsIgnoreCase(txType)) {
            return TxControl.staleRo();
        }
        if (SNAPSHOTRO.equalsIgnoreCase(txType)) {
            return TxControl.snapshotRo();
        }
        if (ONLINERO.equalsIgnoreCase(txType)) {
            return TxControl.snapshotRo();
        }
        throw new IllegalArgumentException("Illegal value for TX control: " + txType);
    }

    private ExecuteDataQuerySettings makeDataQuerySettings() {
        ExecuteDataQuerySettings ret = new ExecuteDataQuerySettings();
        int timeout = getIntegerQueryTimeout();
        if (timeout > 0) {
            ret.setCancelAfter(Duration.ofSeconds(timeout));
            ret.setTimeout(Duration.ofSeconds(timeout+1));
        }
        return ret;
    }

    private ExecuteScanQuerySettings makeScanQuerySettings() {
        ExecuteScanQuerySettings.Builder builder = ExecuteScanQuerySettings.newBuilder();
        int timeout = getIntegerQueryTimeout();
        if (timeout <= 0) {
            timeout = 3600;
        }
        builder.withRequestTimeout(Duration.ofSeconds(timeout));
        return builder.build();
    }

    private ExecuteSchemeQuerySettings makeSchemeQuerySettings() {
        ExecuteSchemeQuerySettings ret = new ExecuteSchemeQuerySettings();
        int timeout = getIntegerQueryTimeout();
        if (timeout > 0) {
            ret.setCancelAfter(Duration.ofSeconds(timeout));
            ret.setTimeout(Duration.ofSeconds(timeout+1));
        }
        return ret;
    }

    private Params makeParams() {
        if (getQueryArguments().trim().length()==0) {
            return Params.create();
        }
        String[] arguments;
        try {
            arguments = CSVSaveService.csvSplitString(getQueryArguments(), COMMA_CHAR);
        } catch(IOException ix) {
            throw new RuntimeException("Failed to parse arguments", ix);
        }
        String[] argumentsTypes = getQueryArgumentsTypes().split(COMMA);
        if (arguments.length != argumentsTypes.length) {
            throw new RuntimeException("number of arguments ("
                    + arguments.length + ") and number of types ("
                    + argumentsTypes.length + ") are not equal");
        }
        Params params = Params.create(arguments.length);
        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i];
            String argumentType = argumentsTypes[i];
            params.put("$p" + Integer.toString(i+1), YdbValueConv.convert(argumentType, argument));
        }
        return params;
    }

    private void appendColumns(StringBuilder sb, ResultSetReader rsr) {
        final int nc = rsr.getColumnCount();
        for (int ic=0; ic<nc; ic++) {
            if (ic>0)
                sb.append("\t");
            sb.append(rsr.getColumnName(ic));
        }
        sb.append("\n");
    }

    private static void val2buf(ValueReader vr, StringBuilder sb) {
        Value<?> v = vr.getValue();
        if (v!=null) {
            if (Type.Kind.OPTIONAL.equals(v.getType().getKind())) {
                if (! v.asOptional().isPresent()) {
                    v = null;
                } else {
                    v = v.asOptional().get();
                }
            }
        }
        if (v==null) {
            sb.append("?");
            return;
        }
        sb.append(v.toString());
    }

    private long appendRows(StringBuilder sb, ResultSetReader rsr, long totalRows) {
        final int nc = rsr.getColumnCount();
        final long maxRows = getIntegerResultSetMaxRows();
        int nrow = 0;
        while (rsr.next()) {
            if (sb.length() >= MAX_STORE_CHARS) {
                break;
            }
            if (maxRows >= 0L && (totalRows + nrow) >= maxRows) {
                break;
            }
            for (int ic=0; ic<nc; ic++) {
                if (ic>0)
                    sb.append("\t");
                val2buf(rsr.getColumn(ic), sb);
            }
            sb.append("\n");
            nrow += 1;
        }
        totalRows += rsr.getRowCount();
        return totalRows;
    }

    private boolean isValuePresent(ValueReader vr) {
        if (vr==null) {
            return false;
        }
        if (Type.Kind.OPTIONAL.equals(vr.getType().getKind())) {
            return vr.getValue().asOptional().isPresent();
        }
        return true;
    }

    private int storeVariables(ResultSetReader rsr, int varPos) {
        if (rsr.getRowCount()==0)
            return 0;
        String[] varnames = getVariableNames().split(COMMA);
        if (varnames.length == 0)
            return 0;
        rsr.setRowIndex(0);
        JMeterVariables jmvars = getThreadContext().getVariables();
        int pos = varPos;
        for (; (pos<varnames.length) && (pos < rsr.getColumnCount()); pos++) {
            String name = varnames[pos].trim();
            if (name.length()==0)
                continue;
            ValueReader vr = rsr.getColumn(pos);
            if (isValuePresent(vr)) {
                final StringBuilder sb = new StringBuilder();
                vr.toString(sb);
                jmvars.putObject(name, sb.toString());
            } else {
                jmvars.putObject(name, null);
            }
        }
        return pos;
    }

    /**
     * @return the integer representation queryTimeout
     */
    public int getIntegerQueryTimeout() {
        int timeout;
        if(StringUtils.isEmpty(queryTimeout)) {
            return 0;
        } else {
            try {
                timeout = Integer.parseInt(queryTimeout);
            } catch (NumberFormatException nfe) {
                timeout = 0;
            }
        }
        return timeout;
    }

    /**
     * @return the queryTimeout
     */
    public String getQueryTimeout() {
        return queryTimeout ;
    }

    /**
     * @param queryTimeout query timeout in seconds
     */
    public void setQueryTimeout(String queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    /**
     * @return the integer representation resultSetMaxRows
     */
    public int getIntegerResultSetMaxRows() {
        int maxrows;
        if(StringUtils.isEmpty(resultSetMaxRows)) {
            return -1;
        } else {
            try {
                maxrows = Integer.parseInt(resultSetMaxRows);
            } catch (NumberFormatException nfe) {
                maxrows = -1;
            }
        }
        return maxrows;
    }

    /**
     * @return the resultSetMaxRows
     */
    public String getResultSetMaxRows() {
        return resultSetMaxRows ;
    }

    /**
     * @param resultSetMaxRows max number of rows to iterate through the ResultSet
     */
    public void setResultSetMaxRows(String resultSetMaxRows) {
        this.resultSetMaxRows = resultSetMaxRows;
    }

    public String getQuery() {
        return query;
    }

    /**
     * @param query
     *            The query to set.
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * @return Returns the dataSource.
     */
    public String getDataSource() {
        return dataSource;
    }

    /**
     * @param dataSource
     *            The dataSource to set.
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * @return Returns the queryType.
     */
    public String getQueryType() {
        return queryType;
    }

    /**
     * @param queryType The queryType to set.
     */
    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getTxType() {
        return txType;
    }

    /**
     * @param txType The transaction isolation mode for DataQuery
     */
    public void setTxType(String txType) {
        this.txType = txType;
    }

    public String getQueryArguments() {
        return queryArguments;
    }

    public void setQueryArguments(String queryArguments) {
        this.queryArguments = queryArguments;
    }

    public String getQueryArgumentsTypes() {
        return queryArgumentsTypes;
    }

    public void setQueryArgumentsTypes(String queryArgumentsType) {
        this.queryArgumentsTypes = queryArgumentsType;
    }

    /**
     * @return the variableNames
     */
    public String getVariableNames() {
        return variableNames;
    }

    /**
     * @param variableNames the variableNames to set
     */
    public void setVariableNames(String variableNames) {
        this.variableNames = variableNames;
    }

    /**
     * @return the resultSetHandler
     */
    public String getResultSetHandler() {
        return resultSetHandler;
    }

    /**
     * @param resultSetHandler the resultSetHandler to set
     */
    public void setResultSetHandler(String resultSetHandler) {
        this.resultSetHandler = resultSetHandler;
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testStarted()
     */
    @Override
    public void testStarted() {
        testStarted("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testStarted(java.lang.String)
     */
    @Override
    public void testStarted(String host) {
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded()
     */
    @Override
    public void testEnded() {
        testEnded("");
    }

    /**
     * {@inheritDoc}
     * @see org.apache.jmeter.testelement.TestStateListener#testEnded(java.lang.String)
     */
    @Override
    public void testEnded(String host) {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(80);
        sb.append("[");
        sb.append(getQueryType());
        sb.append("] ");
        sb.append(getQuery());
        sb.append("\n");
        sb.append(getQueryArguments());
        sb.append("\n");
        sb.append(getQueryArgumentsTypes());
        return sb.toString();
    }

}
