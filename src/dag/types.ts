export interface XComHelper {
  /** Push a value under key — available to downstream tasks via pull() */
  push: (key: string, value: unknown) => Promise<void>
  /**
   * Pull a value from an upstream task.
   * - Non-mapped task: returns the single pushed value.
   * - Mapped task: returns an array of all instances' values ordered by map_index.
   */
  pull: (fromTaskId: string, key: string) => Promise<unknown>
}

export interface ConnectionHelper {
  /** Retrieve a connection by conn_id. Returns null if not found. Decrypts in worker. */
  get: (connId: string) => Promise<{
    conn_id: string
    conn_type: string
    host: string | null
    port: number | null
    schema: string | null
    login: string | null
    password: string | null
    extra: Record<string, unknown> | null
  } | null>
}

export interface VariableHelper {
  /** Retrieve a variable value by key. Returns null if not found. Decrypts secrets in worker. */
  get: (key: string) => Promise<string | null>
}

export interface TaskContext {
  dagId: string
  runId: string
  taskId: string
  /** For mapped task instances: the 0-based index of this instance. Null for non-mapped tasks. */
  mapIndex: number | null
  /** For mapped task instances: the input value for this instance. Null for non-mapped tasks. */
  mapValue: unknown
  /**
   * Trigger-time configuration passed by the caller via POST /dags/:id/trigger.
   * Empty object for scheduled/backfill runs (no caller-supplied conf).
   * Read-only — tasks should not mutate this object.
   */
  conf: Record<string, unknown>
  xcom: XComHelper
  connections: ConnectionHelper
  variables: VariableHelper
}

export interface TaskDefinition {
  dependsOn?: string[]
  group?: string           // optional TaskGroup membership — label only, no scheduler impact
  /** Resource pool name — limits concurrency for this task across all runs. */
  pool?: string
  retries?: number        // max retry attempts (default: 0 = no retries)
  retryDelay?: number     // ms to wait before requeuing (default: 0)
  timeout?: number        // ms before worker is killed and task marked failed (default: no timeout)
  run?: (ctx: TaskContext) => Promise<unknown>
  /**
   * Literal expand (Branch A): fan out this task over a static array of values.
   * One task_instance is created per value at run-creation time.
   * ctx.mapIndex (0-based) and ctx.mapValue are injected into each instance.
   * Downstream tasks that depend_on a mapped task wait for ALL instances to succeed.
   *
   * Branch B (XCom-driven dynamic expand) is a planned future extension.
   */
  expand?: unknown[]

  /**
   * Sensor mode: if present, this task polls a condition instead of running once.
   * Return true → task succeeds; return false → task requeues after pokeInterval.
   * `run` should be omitted for sensor tasks.
   */
  poke?: (ctx: TaskContext) => Promise<boolean>
  /** ms between poke attempts when poke() returns false. Default: 30 000 (30s). Min: 1 000. */
  pokeInterval?: number
  /** ms total deadline for sensor; exceeding it marks the task failed. Default: 3 600 000 (1h). */
  sensorTimeout?: number

  /**
   * Shell task: run a shell command instead of a JS function.
   * The command string is passed to the interpreter via `-c`.
   *
   * Supported interpreters (must be installed in the runtime environment):
   *   'bash'  — default; pre-installed in the official airflow-nodejs image (Debian slim)
   *   'sh'    — always available (POSIX shell)
   *   'zsh'   — install via apt-get if available
   *   'tcsh'  — install via apt-get if available
   *   Any absolute path, e.g. '/usr/bin/python3'
   *   'zsh'   — if installed
   *   'tcsh'  — if installed
   *   'fish'  — if installed
   *   Any absolute path, e.g. '/usr/local/bin/python3'
   *
   * stdout/stderr are captured and written to task logs.
   * Exit code 0 = success; non-zero = failure (message includes exit code + stderr).
   *
   * Environment variables available inside the command:
   *   DAG_ID, RUN_ID, TASK_ID  — from the current task context
   *
   * Cannot be combined with `run` or `poke`.
   */
  shell?: {
    command: string
    /** Shell interpreter binary name or absolute path. Default: 'bash' */
    interpreter?: string
    /** Working directory for the command. Default: process.cwd() */
    cwd?: string
    /** Additional environment variables merged with process.env */
    env?: Record<string, string>
    /** Timeout in ms (overrides task-level timeout for shell execution). */
    timeout?: number
  }

  /**
   * Python task: run inline Python code or a .py script file.
   *
   * Provide exactly one of `code` (inline) or `script` (file path).
   *
   * Inline code example:
   *   python: { code: 'print("hello from python")' }
   *
   * Script file example (path relative to cwd or absolute):
   *   python: { script: '/app/dags/scripts/my_job.py', args: ['--env', 'prod'] }
   *
   * Environment variables injected automatically:
   *   DAG_ID, RUN_ID, TASK_ID  — readable via os.environ
   *
   * Interpreter defaults to 'python3'. Use 'python' or an absolute path to
   * target a specific installation.
   *
   * stdout/stderr are captured line-by-line to task logs.
   * Exit code 0 = success; non-zero = failure.
   *
   * NOTE: python3 is NOT included in the default airflow-nodejs image.
   * Use the python variant image: docker build -f Dockerfile.python -t airflow-nodejs:python .
   * Or set interpreter to any python binary already present in your runtime.
   *
   * Cannot be combined with `run`, `poke`, or `shell`.
   */
  python?: {
    /** Inline Python code string — passed via `python3 -c`. Mutually exclusive with `script`. */
    code?: string
    /** Path to a .py script file — passed as positional arg. Mutually exclusive with `code`. */
    script?: string
    /** Extra positional arguments appended after the script path (ignored for inline code). */
    args?: string[]
    /** Python interpreter binary name or absolute path. Default: 'python3' */
    interpreter?: string
    /** Working directory for the Python process. Default: process.cwd() */
    cwd?: string
    /** Additional environment variables merged with process.env */
    env?: Record<string, string>
    /** Timeout in ms. Default: task-level timeout or no timeout. */
    timeout?: number
  }

  /**
   * Java task: run a .jar file or a class from a classpath.
   *
   * The JRE/JDK must be present in the runtime environment — Java is NOT bundled
   * in the default airflow-nodejs images due to size (~250 MB). Options:
   *   1. Use a custom base image that includes Java
   *   2. Volume-mount a JRE into the container and set `java.binary`
   *   3. Run the container on a host that has Java installed and set `java.binary`
   *
   * JAR example:
   *   java: { jar: '/app/dags/jobs/my-etl.jar', args: ['--date', '2024-01-01'] }
   *
   * Class + classpath example:
   *   java: { mainClass: 'com.example.MyJob', classpath: ['/app/lib/my-lib.jar', '/app/classes'] }
   *
   * Environment variables injected automatically:
   *   DAG_ID, RUN_ID, TASK_ID  — readable via System.getenv()
   *
   * stdout/stderr are captured line-by-line to task logs.
   * Exit code 0 = success; non-zero = failure.
   *
   * Cannot be combined with `run`, `poke`, `shell`, or `python`.
   */
  java?: {
    /** Path to a .jar file to execute (mutually exclusive with mainClass). */
    jar?: string
    /** Fully-qualified main class name (mutually exclusive with jar). Requires classpath. */
    mainClass?: string
    /** Classpath entries (files or directories) joined with ':'. Used with mainClass. */
    classpath?: string[]
    /** Extra arguments passed after the jar/class. */
    args?: string[]
    /** JVM flags passed before the jar/class, e.g. ['-Xmx512m', '-Denv=prod'] */
    jvmArgs?: string[]
    /** Java binary path. Default: 'java' (must be on PATH or set to absolute path). */
    binary?: string
    /** Working directory. Default: process.cwd() */
    cwd?: string
    /** Additional environment variables merged with process.env */
    env?: Record<string, string>
    /** Timeout in ms. Default: task-level timeout or no timeout. */
    timeout?: number
  }

  /**
   * Container task: run the task inside a Docker container.
   * The server must have access to the Docker socket:
   *   docker run -v /var/run/docker.sock:/var/run/docker.sock \
   *              --group-add $(stat -c %g /var/run/docker.sock) \
   *              airflow-nodejs:local
   *
   * The container is ephemeral (--rm). It exits when the command finishes.
   * Exit code 0 = success; non-zero = failure.
   * stdout/stderr are captured line-by-line to task logs.
   *
   * Environment variables injected automatically:
   *   DAG_ID, RUN_ID, TASK_ID  — same as shell/python/java tasks
   *
   * Example:
   *   container: {
   *     image: 'python:3.13-slim',
   *     command: ['python', '-c', 'print("hello from container")'],
   *   }
   *
   * XCom between container tasks: not supported (container has no MongoDB access).
   * Use shared volumes or environment variables to pass data between tasks.
   *
   * Cannot be combined with `run`, `poke`, `shell`, `python`, or `java`.
   */
  container?: {
    /**
     * Docker image to run. Must be pullable in the runtime environment.
     * On Walmart network use the internal mirror prefix:
     *   generic.ci.artifacts.walmart.com/hub-docker-release-remote/<image>
     */
    image: string
    /** Command + args passed to the container. Overrides the image's default CMD. */
    command?: string[]
    /** Additional environment variables merged with DAG_ID/RUN_ID/TASK_ID. */
    env?: Record<string, string>
    /**
     * Volume mounts: host-path:container-path pairs, e.g.
     *   ['/data/input:/input', '/data/output:/output']
     * The dags/ volume is NOT automatically mounted — add it here if needed.
     */
    volumes?: string[]
    /** Working directory inside the container. */
    workdir?: string
    /** docker run --network value. Default: 'bridge'. */
    network?: string
    /**
     * Port mappings: host-port:container-port pairs, e.g.
     *   ['8888:8888', '5000:5000']
     *   ['127.0.0.1:8888:8888']   — bind to localhost only (more secure)
     *
     * Mapped ports are accessible on the host while the container is running.
     * Useful for Jupyter notebooks, web servers, debuggers, etc.
     *
     * Example — Jupyter session accessible at http://localhost:8888:
     *   ports: ['8888:8888']
     *   command: ['jupyter', 'notebook', '--ip=0.0.0.0', '--no-browser',
     *             '--NotebookApp.token=', '--NotebookApp.password=']
     */
    ports?: string[]
    /**
     * Memory limit for the container.
     * Accepts Docker memory strings: '512m', '2g', '1024m', etc.
     * Maps to `docker run --memory`.
     * Default: no limit (uses host available memory).
     *
     * Example: memory: '2g'   → container OOMs and exits if it exceeds 2 GB
     */
    memory?: string
    /**
     * Memory + swap limit. Must be >= memory.
     * '0' disables swap. Default: 2× memory (Docker default).
     * Maps to `docker run --memory-swap`.
     *
     * Example: memorySwap: '2g'  → same as memory, effectively disables swap
     */
    memorySwap?: string
    /**
     * Number of CPUs the container may use (fractional allowed).
     * Maps to `docker run --cpus`.
     * Default: no limit (uses all available CPUs).
     *
     * Example: cpus: '0.5'  → container gets at most half a CPU
     */
    cpus?: string
    /**
     * Writable layer (disk) size limit for the container filesystem.
     * Only supported on overlay2 storage driver with dm or xfs quota.
     * Accepts Docker size strings: '10g', '500m'.
     * Maps to `docker run --storage-opt size=<value>`.
     * Default: no limit.
     *
     * Note: requires the Docker daemon to be configured with storage quotas.
     * On most dev setups this is a no-op; use volumes for reliable disk limits.
     *
     * Example: storageSize: '10g'
     */
    storageSize?: string
    /** Timeout in ms. Default: task-level timeout or no timeout. */
    timeout?: number
  }

  /**
   * Kubernetes task: run the task as an ephemeral Pod on a Kubernetes cluster.
   *
   * Requires `kubectl` on PATH, configured with a valid kubeconfig.
   * The Pod is created with `--restart=Never --rm --attach` so kubectl blocks
   * until the Pod exits, streams stdout/stderr to task logs, and auto-deletes.
   *
   * Exit code 0 = success; non-zero = failure.
   *
   * Supported clusters (anything kubectl can reach):
   *   - Local:  minikube, kind, k3d, Rancher Desktop
   *   - AWS:    EKS  (aws eks update-kubeconfig)
   *   - GCP:    GKE  (gcloud container clusters get-credentials)
   *   - Azure:  AKS  (az aks get-credentials)
   *   - Anywhere with a valid kubeconfig / in-cluster service account
   *
   * Environment variables injected automatically (same as container tasks):
   *   DAG_ID, RUN_ID, TASK_ID
   *
   * NOTE: `ports` is intentionally NOT supported — kubectl does not have a
   * host-port mapping flag equivalent to `docker run -p`. Use a Service or
   * port-forward separately if you need port access to a long-running Pod.
   *
   * Example:
   *   kubernetes: {
   *     image: 'python:3.13-slim',
   *     command: ['python3', '-c', 'print("hello from k8s")'],
   *     namespace: 'airflow',
   *     memory: '512Mi',
   *     cpu: '500m',
   *   }
   *
   * Cannot be combined with `run`, `poke`, `shell`, `python`, `java`, or `container`.
   */
  kubernetes?: {
    /** Container image to run. */
    image: string
    /**
     * Command + args override (equivalent to container's `command`).
     * Passed after `--` to kubectl run.
     */
    command?: string[]
    /**
     * Kubernetes namespace. Default: 'default'.
     * Override via KUBECTL_NAMESPACE env var or set here.
     */
    namespace?: string
    /**
     * Pod name prefix. RFC-1123 safe characters only (lowercase, hyphens).
     * The executor appends a unique suffix automatically.
     * Default: 'airflow-task'.
     */
    podName?: string
    /**
     * Memory request AND limit (same value for both).
     * Kubernetes memory format: '512Mi', '2Gi', '256Mi'.
     * Maps to --requests=memory=<value> --limits=memory=<value>.
     */
    memory?: string
    /**
     * CPU request AND limit (same value for both).
     * Kubernetes CPU format: '500m' (millicores) or '1' (cores).
     * Maps to --requests=cpu=<value> --limits=cpu=<value>.
     */
    cpu?: string
    /**
     * Service account name to bind to the Pod.
     * Useful for granting AWS/GCP IAM via IRSA/Workload Identity.
     */
    serviceAccount?: string
    /** Additional environment variables merged with DAG_ID/RUN_ID/TASK_ID. */
    env?: Record<string, string>
    /** kubeconfig file path. Default: ~/.kube/config (kubectl default). */
    kubeconfig?: string
    /** kubectl context to use. Default: current context in kubeconfig. */
    context?: string
    /** Timeout in ms. Default: task-level timeout or no timeout. */
    timeout?: number
  }

  /**
   * Human-in-the-Loop: when true, the task parks at 'queued' until a human
   * approves or rejects via POST /hitl/:runId/:taskId.
   * Approved → task executes (or succeeds immediately if no `run` body).
   * Rejected → task marked 'failed'; run fails.
   */
  requiresApproval?: boolean
  /** Optional prompt shown to the approver in the UI / API. */
  hitlPrompt?: string
}

/**
 * A named group of tasks. Tasks declare membership via `group: 'groupId'`.
 * Groups can declare dependencies on other groups — the loader expands these
 * into task-level `depends_on` edges before registration.
 */
export interface TaskGroupDefinition {
  /** Human-readable label shown in the UI */
  label?: string
  /** This group's tasks wait until all tasks in each listed group complete */
  dependsOn?: string[]
}

/**
 * A timetable function computes when the next run should fire.
 *
 * @param lastRunAt  - The time the most recent run was created, or null if no
 *                     runs have been created yet (first ever fire).
 * @returns          - A Date for the next fire time, or null to stop scheduling
 *                     permanently (no more runs will be created).
 *
 * The function is called on every scheduler tick (~5s). It must return quickly
 * and must not throw — exceptions are caught and logged, treated as null.
 *
 * Constraints:
 *  - Granularity floor is the scheduler poll interval (~5s).
 *  - No data-interval semantics (unlike Apache Airflow's Timetable).
 *    Use `lastRunAt` to decide when the next run should occur.
 *  - Cannot be combined with `schedule` (cron). Use one or the other.
 *
 * Examples:
 *
 *   // Every 30 minutes:
 *   timetable: (last) => new Date((last ?? new Date()).getTime() + 30 * 60 * 1000)
 *
 *   // Weekdays only at 09:00 UTC:
 *   timetable: (last) => {
 *     const next = new Date(); next.setUTCHours(9, 0, 0, 0);
 *     if (next <= (last ?? new Date(0))) next.setUTCDate(next.getUTCDate() + 1);
 *     while (next.getUTCDay() === 0 || next.getUTCDay() === 6)
 *       next.setUTCDate(next.getUTCDate() + 1);
 *     return next;
 *   }
 *
 *   // Run 5 times total then stop:
 *   timetable: (last, runCount) => runCount >= 5 ? null :
 *     new Date((last ?? new Date()).getTime() + 10_000)
 */
export type TimetableFn = (lastRunAt: Date | null, runCount: number) => Date | null

export interface DagDefinition {
  id: string
  schedule: string | null  // cron expression, or null for manual-only
  /**
   * Custom timetable function — alternative to cron `schedule`.
   * Cannot be combined with `schedule`. Set `schedule: null` when using timetable.
   * See TimetableFn above for full documentation and examples.
   */
  timetable?: TimetableFn
  sla?: number             // ms — if a run hasn't completed within this window, an SLA alert is fired
  version?: string         // sha256[:12] of the dag source file — stamped by the loader
  tasks: Record<string, TaskDefinition>
  /** Optional TaskGroup definitions. Tasks opt-in via task.group = 'groupId'. */
  groups?: Record<string, TaskGroupDefinition>
  /**
   * Dataset URIs this dag PRODUCES when it completes successfully.
   * e.g. ['s3://bucket/users/', 'pg://mydb/orders']
   */
  outlets?: string[]
  /**
   * Dataset URIs this dag CONSUMES. The dag runs when ALL listed datasets have
   * received a new event since the last trigger (AND-semantics).
   * A dag with `datasets` keeps `schedule: null` — cron scheduling is ignored.
   */
  datasets?: string[]

  /**
   * Docker images required by container tasks in this dag.
   * Each entry is either:
   *   - A path to a .tar file (relative to dags/ or absolute):
   *       './images/python-3.13-slim.tar'
   *     The server runs `docker load -i <path>` when the dag is loaded.
   *     Users export images offline: docker save python:3.13-slim -o python-3.13-slim.tar
   *
   *   - A plain image name (must already be present in the local Docker daemon):
   *       'python:3.13-slim'
   *     No action taken — the image is expected to exist at task execution time.
   *
   * Loading is idempotent — if the image from the tar is already present,
   * docker load is a no-op (fast check, no re-extraction).
   *
   * If the Docker socket is not available, load is skipped with a warning.
   */
  requiredImages?: string[]

  /**
   * URL to POST to when a run completes successfully.
   * Payload: { dag_id, run_id, state: 'success', logical_date, conf, tags, ended_at }
   * Delivery is fire-and-forget with a 5s timeout — failures are logged, not retried.
   * Only trusted authors can set this; never accept caller-supplied URLs at trigger time.
   */
  onSuccess?: string

  /**
   * URL to POST to when a run fails (any task failed).
   * Same payload shape as onSuccess, with state: 'failed'.
   */
  onFailure?: string
}

/** Helper to define a Dag with full TypeScript inference */
export function dag(def: DagDefinition): DagDefinition {
  return def
}
