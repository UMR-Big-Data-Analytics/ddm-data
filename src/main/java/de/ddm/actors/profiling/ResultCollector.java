package de.ddm.actors.profiling;

import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.Guardian;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ResultCollector extends AbstractBehavior<ResultCollector.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Message {
		private static final long serialVersionUID = -7070569202900845736L;
		List<InclusionDependency> inclusionDependencies;
	}

	@NoArgsConstructor
	public static class FinalizeMessage implements Message {
		private static final long serialVersionUID = -6603856949941810321L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "resultCollector";

	public static Behavior<Message> create() {
		return Behaviors.setup(ResultCollector::new);
	}

	private ResultCollector(ActorContext<Message> context) throws IOException {
		super(context);

		File file = new File(DomainConfigurationSingleton.get().getResultCollectorOutputFileName());
		if (file.exists() && !file.delete())
			throw new IOException("Could not delete existing result file: " + file.getName());
		if (!file.createNewFile())
			throw new IOException("Could not create result file: " + file.getName());

		this.writer = new BufferedWriter(new FileWriter(file));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final BufferedWriter writer;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ResultMessage.class, this::handle)
				.onMessage(FinalizeMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ResultMessage message) throws IOException {
		this.getContext().getLog().info("Received {} INDs!", message.getInclusionDependencies().size());

		for (InclusionDependency ind : message.getInclusionDependencies()) {
			this.writer.write(ind.toString());
			this.writer.newLine();
		}

		return this;
	}

	private Behavior<Message> handle(FinalizeMessage message) throws IOException {
		this.getContext().getLog().info("Received FinalizeMessage!");

		this.writer.flush();
		this.getContext().getSystem().unsafeUpcast().tell(new Guardian.ShutdownMessage());
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.writer.close();
		return this;
	}
}
