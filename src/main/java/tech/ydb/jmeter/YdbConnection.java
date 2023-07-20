package tech.ydb.jmeter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.auth.StaticCredentials;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.core.grpc.GrpcTransportBuilder;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;

/**
 *
 * @author zinal
 */
public class YdbConnection implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(YdbConnection.class);

    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final String endpoint;
    private final String database;
    private final YdbConfigElement.AuthMode authMode;

    public YdbConnection(YdbConfigElement config) {
        GrpcTransportBuilder builder = GrpcTransport
                .forEndpoint(config.getEndpoint(), config.getDatabase());
        switch (config.getAuthModeCode()) {
            case ENV:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getAuthProviderFromEnviron());
                break;
            case METADATA:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getMetadataAuthProvider());
                break;
            case SAKEY:
                builder = builder.withAuthProvider(
                        CloudAuthHelper.getServiceAccountFileAuthProvider(config.getSaKeyFile()));
                break;
            case STATIC:
                builder = builder.withAuthProvider(
                    new StaticCredentials(config.getUsername(), config.getPassword()));
                break;
            case NONE:
                break;
        }
        String tlsCertFile = config.getTlsCertFile();
        if (! StringUtils.isEmpty(tlsCertFile)) {
            byte[] cert;
            try {
                cert = Files.readAllBytes(Paths.get(tlsCertFile));
            } catch(IOException ix) {
                throw new RuntimeException("Failed to read file " + tlsCertFile, ix);
            }
            builder.withSecureConnection(cert);
        }
        GrpcTransport gt = builder.build();
        try {
            this.tableClient = TableClient.newClient(gt)
                    .sessionPoolSize(1, config.getPoolMaxInt())
                    .build();
            this.retryCtx = SessionRetryContext.create(tableClient).idempotent(true).build();
            this.endpoint = config.getEndpoint();
            this.authMode = config.getAuthModeCode();
            this.database = gt.getDatabase();
            this.transport = gt;
            gt = null;
        } finally {
            if (gt != null)
                gt.close();
        }
    }

    public TableClient getTableClient() {
        return tableClient;
    }

    public SessionRetryContext getRetryCtx() {
        return retryCtx;
    }

    public String getDatabase() {
        return database;
    }

    @Override
    public void close() {
        if (tableClient != null) {
            try {
                tableClient.close();
            } catch(Exception ex) {
                LOG.warn("TableClient closing threw an exception", ex);
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch(Exception ex) {
                LOG.warn("GrpcTransport closing threw an exception", ex);
            }
        }
    }

    public String getConnectionInfo() {
        StringBuilder builder = new StringBuilder(100);
        builder.append(", endpoint:").append(endpoint)
            .append(", database:").append(database)
            .append(", authMode:").append(authMode.name());
        return builder.toString();
    }

}
