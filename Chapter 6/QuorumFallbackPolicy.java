import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.*;

public class QuorumFallbackPolicy implements RetryPolicy {
    private RetryPolicy defaultPolicy =
            DowngradingConsistencyRetryPolicy.INSTANCE;
    public static final RetryPolicy INSTANCE = new QuorumFallbackPolicy();

    private QuorumFallbackPolicy() {}

    public void init(Cluster cluster) { defaultPolicy.init(cluster); }
    public void close() { defaultPolicy.close(); }

    @Override
    public RetryDecision onUnavailable(Statement stmt,
                                       ConsistencyLevel cl,
                                       int reqRep,int aliveRep,
                                       int nbRetry){

        if (nbRetry == 0 && ConsistencyLevel.LOCAL_QUORUM == cl)
            return RetryDecision.retry(ConsistencyLevel.QUORUM);
        else if (nbRetry == 1)
            return RetryDecision.retry(ConsistencyLevel.ONE);
        else
            return defaultPolicy.onUnavailable(stmt, cl,reqRep,
                    aliveRep, nbRetry);
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmt,
                                       ConsistencyLevel cl, int reqRes, int recRes,
                                       boolean dataRet, int nbRetry) {
        return defaultPolicy.onReadTimeout(stmt, cl, reqRes,
                recRes, dataRet, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmt,
                                        ConsistencyLevel cl, WriteType type, int reqAcks, int
            recAcks, int nbRetry) {
        return defaultPolicy.onWriteTimeout(stmt, cl, type,
                reqAcks, recAcks, nbRetry);
    }

    @Override
    public RetryDecision onRequestError(Statement stmt,
                                        ConsistencyLevel cl,
                                        DriverException exc,
                                        int nbRetry){
        return defaultPolicy.onRequestError(stmt, cl, exc, nbRetry);
    }
}
