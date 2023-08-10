package tech.ydb.jmeter;

import tech.ydb.core.StatusCode;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.SessionRetryHandler;

/**
 *
 * @author mzinal
 */
public class YdbRetryHandler implements SessionRetryHandler {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(YdbRetryHandler.class);

    private final String queryId;
    private int retryCount;

    public YdbRetryHandler(String queryId) {
        this.queryId = queryId;
    }

    public int getRetryCount() {
        return retryCount;
    }

    @Override
    public void onSuccess(SessionRetryContext context, int retryNumber, long millis) {
        this.retryCount = retryNumber;
        if (retryNumber > 0) {
            LOGGER.debug("Query [{}] completed with {} retries in {} ms.",
                    queryId, retryNumber, millis);
        }
        SessionRetryHandler.super.onSuccess(context, retryNumber, millis);
    }

    @Override
    public void onCancel(SessionRetryContext context, int retryNumber, long millis) {
        this.retryCount = retryNumber;
        LOGGER.info("Query [{}] cancelled with {} retries in {} ms.",
                queryId, retryNumber, millis);
        SessionRetryHandler.super.onCancel(context, retryNumber, millis);
    }

    @Override
    public void onLimit(SessionRetryContext context, StatusCode code, int retryLimit, long millis) {
        this.retryCount = retryLimit;
        LOGGER.info("Query [{}] exceeded retry limit of {} retries in {} ms.",
                queryId, retryLimit, millis);
        SessionRetryHandler.super.onLimit(context, code, retryLimit, millis);
    }

    @Override
    public void onError(SessionRetryContext context, StatusCode code, int retryNumber, long millis) {
        this.retryCount = retryNumber;
        LOGGER.info("Query [{}] failed with non-retryable status {} at {} retries in {} ms.",
                queryId, code, retryNumber, millis);
        SessionRetryHandler.super.onError(context, code, retryNumber, millis);
    }

    @Override
    public void onError(SessionRetryContext context, Throwable issue, int retryNumber, long millis) {
        this.retryCount = retryNumber;
        LOGGER.info("Query [{}] failed with non-retryable exception at {} retries in {} ms.",
                queryId, retryNumber, millis, issue);
        SessionRetryHandler.super.onError(context, issue, retryNumber, millis);
    }

}
