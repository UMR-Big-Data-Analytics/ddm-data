package de.ddm.actors.patterns;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LargeMessageProxy extends AbstractBehavior<LargeMessageProxy.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = DependencyWorker.DataMessage.class, name = "DataMessage")}
    )
    public interface LargeMessage extends AkkaSerializable {
    }

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SendMessage implements Message {
        private static final long serialVersionUID = -1203695340601241430L;
        private LargeMessage message;
        private ActorRef<Message> receiverProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectMessage implements Message {
        private static final long serialVersionUID = -2368932735326858722L;
        private int senderTransmissionKey;
        private ActorRef<Message> senderProxy;
        private int largeMessageSize;
        private int serializerId;
        private String manifest;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectAckMessage implements Message {
        private static final long serialVersionUID = 6497424731575554980L;
        private int senderTransmissionKey;
        private int receiverTransmissionKey;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage implements Message {
        private static final long serialVersionUID = -8435193720156121630L;
        private byte[] bytes;
        private int senderTransmissionKey;
        private int receiverTransmissionKey;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesAckMessage implements Message {
        private static final long serialVersionUID = 5992096322167014051L;
        private int senderTransmissionKey;
        private int receiverTransmissionKey;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static int MAX_MESSAGE_SIZE = 100000;

    public static Behavior<Message> create(ActorRef<LargeMessage> parent) {
        return Behaviors.setup(context -> new LargeMessageProxy(context, parent));
    }

    private LargeMessageProxy(ActorContext<Message> context, ActorRef<LargeMessage> parent) {
        super(context);

        this.parent = parent;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef<LargeMessage> parent;

    private int messageCounter = 0;

    private final Map<Integer, SendState> pendingSends = new HashMap<>();
    private final Map<Integer, ReceiveState> pendingReceives = new HashMap<>();

    private final Serialization serialization = SerializationExtension.get(this.getContext().getSystem());

    @Data
    @AllArgsConstructor
    private static class SendState {
        private byte[] bytes;
        private int offset;
        private ActorRef<Message> receiverProxy;
    }

    @Data
    @AllArgsConstructor
    private static class ReceiveState {
        private byte[] bytes;
        private int offset;
        private ActorRef<Message> senderProxy;
        private int serializerId;
        private String manifest;
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(SendMessage.class, this::handle)
                .onMessage(ConnectMessage.class, this::handle)
                .onMessage(ConnectAckMessage.class, this::handle)
                .onMessage(BytesMessage.class, this::handle)
                .onMessage(BytesAckMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(SendMessage message) {
        LargeMessage largeMessage = message.getMessage();

        byte[] bytes = this.serialization.serialize(largeMessage).get();
        int serializerId = this.serialization.findSerializerFor(largeMessage).identifier();
        String manifest = Serializers.manifestFor(this.serialization.findSerializerFor(largeMessage), largeMessage);

        int senderTransmissionKey = this.messageCounter++;
        this.pendingSends.put(senderTransmissionKey, new SendState(bytes, 0, message.getReceiverProxy()));

        message.getReceiverProxy().tell(new ConnectMessage(senderTransmissionKey, this.getContext().getSelf(),
                bytes.length, serializerId, manifest));
        return this;
    }

    private Behavior<Message> handle(ConnectMessage message) {
        int receiverTransmissionKey = this.messageCounter++;
        this.pendingReceives.put(receiverTransmissionKey, new ReceiveState(new byte[message.getLargeMessageSize()], 0
                , message.getSenderProxy(), message.getSerializerId(), message.getManifest()));

        message.getSenderProxy().tell(new ConnectAckMessage(message.getSenderTransmissionKey(),
                receiverTransmissionKey));
        return this;
    }

    private Behavior<Message> handle(ConnectAckMessage message) {
        return this.sendNext(message.getSenderTransmissionKey(), message.getReceiverTransmissionKey());
    }

    private Behavior<Message> handle(BytesAckMessage message) {
        return this.sendNext(message.getSenderTransmissionKey(), message.getReceiverTransmissionKey());
    }

    private Behavior<Message> sendNext(int senderTransmissionKey, int receiverTransmissionKey) {
        ActorRef<Message> receiverProxy = this.pendingSends.get(senderTransmissionKey).getReceiverProxy();

        SendState state = this.pendingSends.get(senderTransmissionKey);

        byte[] bytes = state.getBytes();
        int startOffset = state.getOffset();
        int endOffset = Math.min(startOffset + MAX_MESSAGE_SIZE, bytes.length);

        byte[] nextBytes = Arrays.copyOfRange(bytes, startOffset, endOffset);
        state.setOffset(endOffset);

        if (endOffset == bytes.length)
            this.pendingSends.remove(senderTransmissionKey);

        receiverProxy.tell(new BytesMessage(nextBytes, senderTransmissionKey, receiverTransmissionKey));
        return this;
    }

    private Behavior<Message> handle(BytesMessage message) {
        ReceiveState receiveState = this.pendingReceives.get(message.getReceiverTransmissionKey());

        byte[] bytes = receiveState.getBytes();
        int offset = receiveState.getOffset();

        System.arraycopy(message.getBytes(), 0, bytes, offset, message.getBytes().length);

        receiveState.setOffset(offset + message.getBytes().length);

        if (receiveState.getOffset() != bytes.length) {
            receiveState.getSenderProxy().tell(new BytesAckMessage(message.getSenderTransmissionKey(),
                    message.getReceiverTransmissionKey()));
            return this;
        }

        this.pendingReceives.remove(message.getReceiverTransmissionKey());

        LargeMessage largeMessage = (LargeMessage) this.serialization.deserialize(bytes,
                receiveState.getSerializerId(), receiveState.getManifest()).get();

        this.parent.tell(largeMessage);
        return this;
    }

}