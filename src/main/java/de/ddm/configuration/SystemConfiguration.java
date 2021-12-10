package de.ddm.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Data;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Data
public class SystemConfiguration {

	public static final String MASTER_ROLE = "master";
	public static final String WORKER_ROLE = "worker";

	public static final int DEFAULT_MASTER_PORT = 7877;
	public static final int DEFAULT_WORKER_PORT = 7879;

	private String role = MASTER_ROLE;                 // This machine's role in the cluster.

	private String host = getDefaultHost();            // This machine's host name or IP that we use to bind this application against
	private int port = DEFAULT_MASTER_PORT;            // This machines port that we use to bind this application against

	private String masterHost = getDefaultHost();      // The host name or IP of the master; if this is a master, masterHost = host
	private int masterPort = DEFAULT_MASTER_PORT;      // The port of the master; if this is a master, masterPort = port

	private String actorSystemName = "ddm";            // The name of this application

	private int numWorkers = 1;                        // The number of workers to start locally; should be at least one if the algorithm is started standalone (otherwise there are no workers to run the application)

	private boolean startPaused = false;               // Wait for some console input to start; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)

	private boolean hardMode = false;					// Solve the hard version of the task

	private static String getDefaultHost() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "localhost";
		}
	}

	public void update(CommandMaster commandMaster) {
		this.role = MASTER_ROLE;
		this.host = commandMaster.host;
		this.port = commandMaster.port;
		this.masterHost = commandMaster.host;
		this.masterPort = commandMaster.port;
		this.numWorkers = commandMaster.numWorkers;
		this.startPaused = commandMaster.startPaused;
		this.hardMode = commandMaster.hardMode;
	}

	public void update(CommandWorker commandWorker) {
		this.role = WORKER_ROLE;
		this.host = commandWorker.host;
		this.port = commandWorker.port;
		this.masterHost = commandWorker.masterhost;
		this.masterPort = commandWorker.masterport;
		this.numWorkers = commandWorker.numWorkers;
	}

	public Config toAkkaConfig() {
		return ConfigFactory.parseString("" +
				"akka.remote.artery.canonical.hostname = \"" + this.host + "\"\n" +
				"akka.remote.artery.canonical.port = " + this.port + "\n" +
				"akka.cluster.roles = [" + this.role + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + this.actorSystemName + "@" + this.masterHost + ":" + this.masterPort + "\"]")
				.withFallback(ConfigFactory.load("application"));
	}

	public Config toAkkaTestConfig() {
		return ConfigFactory.parseString("" +
				"akka.remote.artery.canonical.hostname = \"" + this.host + "\"\n" +
				"akka.remote.artery.canonical.port = " + this.port + "\n" +
				"akka.cluster.roles = [" + this.role + "]")
				.withFallback(ConfigFactory.load("application"));
	}
}
