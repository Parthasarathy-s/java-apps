import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

public class AccountManager {

    private static final int    FEE_FREE_THRESHOLD = 3;
    private static final double CREDIT_FEE         = 3.0;
    private static final double DEBIT_FEE          = 1.0;

    private final Map<String, LongAdder>  countMap      = new ConcurrentHashMap<>();
    private final Map<String, DoubleAdder> sumMap       = new ConcurrentHashMap<>();
    private final Map<String, DoubleAdder> totalFeeMap  = new ConcurrentHashMap<>();

    public void addTransaction(Transaction t) {
        String id = t.accountId();

        // increment count first, then check threshold
        LongAdder count = countMap.computeIfAbsent(id, k -> new LongAdder());
        count.increment();

        sumMap.computeIfAbsent(id, k -> new DoubleAdder()).add(t.amt());
        totalFeeMap.computeIfAbsent(id, k -> new DoubleAdder());

        if (count.sum() > FEE_FREE_THRESHOLD) {
            double fee = (t.type() == TransactionType.CREDIT) ? CREDIT_FEE : DEBIT_FEE;
            totalFeeMap.get(id).add(fee);
        }
    }

    public double getAverage(String accountId) {
        LongAdder  count = countMap.get(accountId);
        DoubleAdder sum  = sumMap.get(accountId);
        if (count == null || count.sum() == 0) return 0.0;
        return sum.sum() / count.sum();
    }

    public double getTotalFee(String accountId) {
        DoubleAdder fee = totalFeeMap.get(accountId);
        return fee == null ? 0.0 : fee.sum();
    }

    public long getTransactionCount(String accountId) {
        LongAdder count = countMap.get(accountId);
        return count == null ? 0L : count.sum();
    }

    /** Full account summary snapshot */
    public void printSummary(String accountId) {
        System.out.printf(
            "Account: %s | Txns: %d | Avg Amt: %.2f | Total Fees: %.2f%n",
            accountId,
            getTransactionCount(accountId),
            getAverage(accountId),
            getTotalFee(accountId)
        );
    }
}
