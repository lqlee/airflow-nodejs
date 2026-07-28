import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates Java tasks — run a .jar file or a class from a classpath.
 *
 * IMPORTANT: Java is NOT bundled in any airflow-nodejs image (JRE is ~250 MB).
 * Options to provide Java:
 *   1. Build a custom image FROM airflow-nodejs:python with Java installed
 *   2. Volume-mount a JRE and set java.binary to its absolute path
 *   3. Run on a host that has Java on PATH
 *
 * To test locally without a jar, use a shell task with javac + java:
 *   shell: { command: 'javac Hello.java && java Hello' }
 *
 * These examples assume 'java' is on PATH in the runtime environment.
 */
export default dag({
  id: 'java_demo',
  schedule: null,  // manual trigger only
  tasks: {

    // Run a pre-built jar
    run_jar: {
      java: {
        jar: '/app/dags/jobs/my-etl.jar',       // absolute path inside container
        args: ['--date', '2024-01-01'],
        jvmArgs: ['-Xmx512m', '-Denv=prod'],
        // java.binary defaults to 'java' — set if JRE is not on PATH:
        // binary: '/opt/jre/bin/java',
      }
    },

    // Run a class from a classpath
    run_class: {
      dependsOn: ['run_jar'],
      java: {
        mainClass: 'com.example.MyJob',
        classpath: ['/app/dags/jobs/my-lib.jar', '/app/dags/jobs/classes'],
        args: ['--mode', 'batch'],
        jvmArgs: ['-Xmx256m'],
        env: { APP_ENV: 'production', REGION: 'us-central1' },
      }
    },

  }
});
