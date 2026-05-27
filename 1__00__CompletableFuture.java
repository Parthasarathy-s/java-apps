import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ApprovalWorkflowAsync {

    // ─── 1. Declare your own ExecutorService ───────────────────────────
    private static final ExecutorService ioExecutor =
            Executors.newFixedThreadPool(
                    10,
                    new ThreadFactory() {
                        private final AtomicInteger count = new AtomicInteger(1);
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r);
                            t.setName("approval-io-" + count.getAndIncrement());
                            t.setDaemon(true); // won't block JVM shutdown
                            return t;
                        }
                    }
            );

    public static void main(String[] args) throws Exception {

        // ─── 2. Build the async pipeline ──────────────────────────────
        CompletableFuture<String> pipeline =

                // Stage 1: fetch user from DB (IO bound)
                CompletableFuture.supplyAsync(
                                () -> fetchUser(101),
                                ioExecutor                          // <── your pool, not common pool
                        )

                        // Stage 2: transform the result (light CPU work)
                        .thenApply(user -> {
                            System.out.println("[thenApply] Thread: " + Thread.currentThread().getName());
                            return "ENRICHED_" + user;
                        })

                        // Stage 3: call Workflow REST (IO bound again)
                        .thenApplyAsync(
                                ApprovalWorkflowAsync::callWorkflow,
                                ioExecutor                          // <── explicitly on your pool
                        )

                        // Stage 4: handle any failure gracefully
                        .exceptionally(ex -> {
                            System.out.println("Pipeline failed: " + ex.getMessage());
                            return "FALLBACK_RESPONSE";
                        });

        // ─── 3. Block ONLY at the boundary (e.g. HTTP response) ───────
        String finalResult = pipeline.get(5, TimeUnit.SECONDS);
        System.out.println("Final result: " + finalResult);

        // ─── 4. Always shut down your executor cleanly ─────────────────
        ioExecutor.shutdown();
    }

    // ── Simulated IO calls ─────────────────────────────────────────────
    static String fetchUser(int id) {
        System.out.println("[fetchUser] Thread: " + Thread.currentThread().getName());
        sleep(500); // simulate DB query
        return "USER_" + id;
    }

    static String callWorkflow(String user) {
        System.out.println("[callWorkflow] Thread: " + Thread.currentThread().getName());
        sleep(300); // simulate REST call
        return "WORKFLOW_TASK_FOR_" + user;
    }

    static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
