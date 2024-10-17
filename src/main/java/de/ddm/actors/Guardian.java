package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop$;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.Reaper;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class Guardian extends AbstractBehavior<Guardian.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -6896669928271349802L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
		private ActorRef<Message> initiator;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = 2336368568740749020L;
		Receptionist.Listing listing;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "userGuardian";

	public static final ServiceKey<Guardian.Message> guardianService = ServiceKey.create(Guardian.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(
				context -> Behaviors.withTimers(timers -> new Guardian(context, timers)));
	}

	private Guardian(ActorContext<Message> context, TimerScheduler<Message> timer) {
		super(context);

		this.timer = timer;

		this.reaper = context.spawn(Reaper.create(), Reaper.DEFAULT_NAME);
		this.master = this.isMaster() ? context.spawn(Master.create(), Master.DEFAULT_NAME) : null;
		this.worker = context.spawn(Worker.create(), Worker.DEFAULT_NAME);

		context.getSystem().receptionist().tell(Receptionist.register(guardianService, context.getSelf()));

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(guardianService, listingResponseAdapter));
	}

	private boolean isMaster() {
		return SystemConfigurationSingleton.get().getRole().equals(SystemConfiguration.MASTER_ROLE);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final TimerScheduler<Message> timer;

	private Set<ActorRef<Message>> userGuardians = new HashSet<>();

	private final ActorRef<Reaper.Message> reaper;
	private ActorRef<Master.Message> master;
	private ActorRef<Worker.Message> worker;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		if (this.master != null)
			this.master.tell(new Master.StartMessage());
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		ActorRef<Message> self = this.getContext().getSelf();

		if ((message.getInitiator() != null) || this.isClusterDown()) {
			this.shutdown();
		} else {
			for (ActorRef<Message> userGuardian : this.userGuardians)
				if (!userGuardian.equals(self))
					userGuardian.tell(new ShutdownMessage(self));

			if (!this.timer.isTimerActive("ShutdownReattempt"))
				this.timer.startTimerAtFixedRate("ShutdownReattempt", message, Duration.ofSeconds(5), Duration.ofSeconds(5));
		}
		return this;
	}

	private boolean isClusterDown() {
		return this.userGuardians.isEmpty() || (this.userGuardians.contains(this.getContext().getSelf()) && this.userGuardians.size() == 1);
	}

	private void shutdown() {
		if (this.worker != null) {
			this.worker.tell(new Worker.ShutdownMessage());
			this.worker = null;
		}
		if (this.master != null) {
			this.master.tell(new Master.ShutdownMessage());
			this.master = null;
		}
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		this.userGuardians = message.getListing().getServiceInstances(Guardian.guardianService);

		if (this.timer.isTimerActive("ShutdownReattempt") && this.isClusterDown())
			this.shutdown();

		return this;
	}
}