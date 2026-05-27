import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/***
    👉 ForkJoinPool
    a chef breaking a big recipe into sub-recipes, cooking in parallel
    ☕️ Calculating portfolio risk across 1 million trades

    👉 ExecutorService
    a call center with N agents, each independently handling one ticket
    ☕️ IO-bound/ DB connection/ Rate limit

    ThreadPoolExecutor pool = new ThreadPoolExecutor(
    5,                          // corePoolSize    — min threads always alive
    20,                         // maximumPoolSize — max threads under load
    60L, TimeUnit.SECONDS,      // keepAliveTime   — idle thread TTL
    new ArrayBlockingQueue<>(500), // workQueue    — bounded queue!
    new ThreadFactory() { ... },   // custom thread naming
    new ThreadPoolExecutor.CallerRunsPolicy() // rejection policy
);

New task arrives
        │
        ├── Core threads free?          YES → assign to core thread
        │
        ├── Core threads all busy?      YES → put in queue
        │
        ├── Queue full?                 YES → create extra thread (up to max 20)
        │
        └── At max threads + queue full?     → REJECTION POLICY kicks in



                    ┌─────────────────────────────────┐
New Tasks ──────►   │         ThreadPoolExecutor       │
                    │                                  │
                    │  Core Threads (always alive)     │
                    │  ┌──────┐ ┌──────┐ ┌──────┐     │
                    │  │  T1  │ │  T2  │ │  T3  │     │
                    │  │ BUSY │ │ BUSY │ │ BUSY │ ... │
                    │  └──────┘ └──────┘ └──────┘     │
                    │                                  │
                    │  Queue (500 seats)               │
                    │  [ t6 | t7 | t8 | t9 | ... ]    │
                    │                                  │
                    │  Extra Threads (born when        │
                    │  queue full, die when idle)      │
                    │  ┌──────┐ ┌──────┐              │
                    │  │  T6  │ │  T7  │ ...up to 20  │
                    │  └──────┘ └──────┘              │
                    └─────────────────────────────────┘
                                    │
                              Queue full
                          + all 20 threads busy
                                    │
                                    ▼
                          CallerRunsPolicy
                    (calling thread runs the task itself)

Executors (factory)
    │
    ├── newFixedThreadPool(n)          → ThreadPoolExecutor (fixed)
    ├── newCachedThreadPool()          → ThreadPoolExecutor (elastic)
    ├── newSingleThreadExecutor()      → ThreadPoolExecutor (single)
    ├── newScheduledThreadPool(n)      → ScheduledThreadPoolExecutor
    ├── newSingleThreadScheduledExecutor() → ScheduledThreadPoolExecutor (single)
    └── newWorkStealingPool(n)         → ForkJoinPool
    
FixedThreadPool — Fixed n threads, Unbounded queue — Best for predictable IO workloads.
CachedThreadPool — 0 to unlimited threads, No queue — Best for bursty short-lived tasks.
SingleThreadExecutor — 1 thread, Unbounded queue — Best for sequential ordered processing.
ScheduledThreadPool — Fixed n threads, Delayed queue — Best for periodic and scheduled jobs.
WorkStealingPool — CPU cores threads, Per-thread queue — Best for independent parallel tasks. (ForkJoinPool - default)
ThreadPoolExecutor — You decide, You decide — Best for production systems needing full control.

java.util.concurrent
        │
        ├── Executor                          (interface) → just execute(Runnable)
        │       │
        │       └── ExecutorService           (interface) → submit(), shutdown(), invokeAll()
        │               │
        │               ├── ThreadPoolExecutor        (concrete) ← newFixedThreadPool uses this internally
        │               │
        │               ├── ScheduledThreadPoolExecutor (concrete) ← for cron-style scheduling
        │               │
        │               └── ForkJoinPool              (concrete) ← work stealing, recursive tasks
        │
        └── Executors                         (factory/helper class) → newFixedThreadPool(), newCachedThreadPool()
 */

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
