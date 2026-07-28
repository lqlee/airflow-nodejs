import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates Java tasks — run a .jar file or a class from a classpath.
 *
 * Requires the java variant image:
 *   ./docker-build.sh --variant java
 *   docker run ... airflow-nodejs:java
 *
 * The java variant bundles OpenJDK 21 JRE headless.
 * To compile .java source, you need a JDK (javac) — the JRE only runs .jar files.
 *
 * dags/jobs/hello.jar is a pre-compiled demo jar (run: javac Hello.java && jar cfe hello.jar Hello Hello.class)
 *
 * Context env vars available via System.getenv(): DAG_ID, RUN_ID, TASK_ID
 */
export default dag({
  id: 'java_demo',
  schedule: null,  // manual trigger only
  tasks: {

    // Run a pre-built jar — simplest and most common usage
    hello_jar: {
      java: {
        jar: '/app/dags/jobs/hello.jar',
        args: ['--mode', 'demo', '--run', 'test'],
        jvmArgs: ['-Xmx128m'],
      }
    },

    // Run a class from an explicit classpath (same jar, different invocation style)
    hello_class: {
      dependsOn: ['hello_jar'],
      java: {
        mainClass: 'Hello',
        classpath: ['/app/dags/jobs/hello.jar'],
        args: ['from-classpath'],
        env: { APP_ENV: 'demo' },
      }
    },

  }
});
