package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.google.common.collect.Streams;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.Column;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class InputReader extends AbstractBehavior<InputReader.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadHeaderMessage implements Message {
		private static final long serialVersionUID = 1729062814525657711L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadBatchMessage implements Message {
		private static final long serialVersionUID = -7915854043207237318L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@AllArgsConstructor
	private static class ColumnTuple {
		private String[] data;
		private String columnName;
	}


	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
		super(context);
		this.id = id;
		this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = InputConfigurationSingleton.get().getHeader(inputFile);
		
		if (InputConfigurationSingleton.get().isFileHasHeader())
			this.reader.readNext();
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();
	private final CSVReader reader;
	private final String[] header;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
		return this;
	}

	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		Column[] columns = new Column[this.header.length];
		for(int i = 0; i < this.header.length; i++){
			columns[i] = new Column(this.id, this.header[i]);
		}

		for (int i = 0; i < this.batchSize; i++) {
			String[] line = this.reader.readNext();
			if (line == null)
				break;
			for(int j = 0; j < line.length; j++){
				columns[j].getValues().add(line[j]);
			}
		}

		if(columns[0].getValues().isEmpty()){
			message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, new ArrayList<>()));
			return this;
		}

//		List<Column> columns = Streams.zip(batch.stream(), Arrays.stream(header), ColumnTuple::new).map(tuple-> new Column(this.id,tuple.columnName,new HashSet<>(Arrays.asList(tuple.data)))).collect(Collectors.toList());
		message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, Arrays.asList(columns)));
		return this;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.reader.close();
		return this;
	}
}
