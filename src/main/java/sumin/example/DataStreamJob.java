package sumin.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sumin.example.window.session.SessionEventTimeWindow;
import sumin.example.window.sliding.SlidingEventTimeWindow;
import sumin.example.window.tumbling.TumblingEventTimeWindow;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env =
				StreamExecutionEnvironment.getExecutionEnvironment();
//		TumblingEventTimeWindow job = new TumblingEventTimeWindow(env);
//		SlidingEventTimeWindow job = new SlidingEventTimeWindow(env);
		SessionEventTimeWindow job = new SessionEventTimeWindow(env);
		job.execute();
	}
}
