import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.*;

public class MyRetryPolicy implements RetryPolicy {
    private RetryPolicy defaultPolicy =
            DowngradingConsistencyRetryPolicy.INSTANCE;

    public MyRetryPolicy() {}

    public void init(Cluster cluster) {}
    public void close() {}

    @Override
    public RetryDecision onReadTimeout(Statement statement,
                                       ConsistencyLevel cl, int requiredResponses,int receivedResponses, boolean dataRetrieved,
                                       int nbRetry) {
        if (nbRetry != 0)
            return RetryDecision.rethrow();
        else if (receivedResponses > 0)
            return RetryDecision.retry(ConsistencyLevel.ONE);
        else
            return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmt,
                                        ConsistencyLevel cl, WriteType type, int reqAcks, int
            recAcks, int nbRetry) {
        return defaultPolicy.onWriteTimeout(stmt, cl, type,
                reqAcks, recAcks, nbRetry);
    }

    @Override
    public RetryDecision onUnavailable(Statement stmt,
                                       ConsistencyLevel cl,
                                       int reqRep,int aliveRep,
                                       int nbRetry){
        return defaultPolicy.onUnavailable(stmt, cl, reqRep,
                aliveRep, nbRetry);
    }

    @Override
    public RetryDecision onRequestError(Statement stmt,
                                       ConsistencyLevel cl,
                                       DriverException exc,
                                       int nbRetry){
        return defaultPolicy.onRequestError(stmt, cl, exc, nbRetry);
    }

}
