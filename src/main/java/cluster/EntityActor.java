package cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.slf4j.Logger;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

class EntityActor extends AbstractBehavior<EntityActor.Command> {
  private final ActorContext<Command> actorContext;
  private final String entityId;
  private final String shardId;
  private final String memberId;
  private final ActorRef<HttpServer.Statistics> httpServerActorRef;

  private State state;
  private CachedItem<?> cachedItem;

  static final EntityTypeKey<Command> entityTypeKey =
      EntityTypeKey.create(Command.class, EntityActor.class.getSimpleName());

  static Behavior<Command> create(
      String entityId,
      ActorRef<HttpServer.Statistics> httpServerActorRef) {
    return Behaviors.setup(context ->
        new EntityActor(context, entityId, httpServerActorRef));
  }

  private EntityActor(
      ActorContext<Command> actorContext,
      String entityId,
      ActorRef<HttpServer.Statistics> httpServerActorRef) {

    super(actorContext);
    this.actorContext = actorContext;
    this.entityId = entityId;
    this.httpServerActorRef = httpServerActorRef;

    int numberOfShards =
        actorContext.getSystem().settings().config()
            .getInt("akka.cluster.sharding.number-of-shards");

    shardId = String.valueOf(Math.abs(entityId.hashCode()) % numberOfShards);
    memberId = actorContext.getSystem().address().toString();

    log().info("Start entity {}", entityId);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(ChangeValue.class, this::onChangeValue)
        .onMessage(CreateCacheEntry.class, this::onCreateCacheEntry)
        .onMessage(GetValue.class, this::onGetValue)
        .onMessage(Passivate.class, msg -> onPassivate())
        .build();
  }

  private Behavior<Command> onChangeValue(ChangeValue command) {
    Value value = command.value;

    if (state == null) {
      state = new State(command.id, value);
      log().info("Initialize {} -> {}", entityId, value);
      command.replyTo.tell(new ChangeValueAck("initialize", command.id, value));
      notifyHttpServer("start", command.replyTo);
    } else {
      log().info("Update {}: {} -> {}", entityId, state.value, value);
      state.value = value;
      command.replyTo.tell(new ChangeValueAck("update", command.id, value));
      notifyHttpServer("ping", command.replyTo);
    }

    return this;
  }

  private Behavior<Command> onCreateCacheEntry(CreateCacheEntry command) {
    if (command.cachedItem == null) {
      log().warn("Cached item is null for {}", command.id);
      command.replyTo.tell(
          new CreateCacheEntryAck("FAILED", command.id, null));
      return this;
    }

    this.cachedItem = command.cachedItem;
    this.state = new State(command.id, new Value(cachedItem.getJsonNode()));

    log().info(
        "Created Cache Entry {} -> {}",
        command.id,
        cachedItem.getJsonNode());

    command.replyTo.tell(
        new CreateCacheEntryAck(
            "CREATED",
            command.id,
            cachedItem.getJsonNode()));

    notifyHttpServer("start", command.replyTo);
    return this;
  }

  private Behavior<Command> onGetValue(GetValue command) {
    if (state == null) {
      command.replyTo.tell(new GetValueAckNotFound(command.id));
      return this;
    }

    command.replyTo.tell(new GetValueAck(command.id, state.value));
    return this;
  }

  private Behavior<Command> onPassivate() {
    log().info(
        "Stop passivate entity={} shard={} member={}",
        entityId,
        shardId,
        memberId);

    notifyHttpServer("stop", null);
    return Behaviors.stopped();
  }

  private void notifyHttpServer(String action, ActorRef<Command> sender) {
    if (httpServerActorRef == null) {
      return;
    }

    final var address = sender == null ? null : addressFor(sender);
    final var entityAction =
        new HttpServer.EntityAction(
            memberId, shardId, entityId, action, address);

    httpServerActorRef.tell(
        new HttpServerActor.BroadcastEntityAction(entityAction));
  }

  private String addressFor(ActorRef<Command> sender) {
    return sender.path().address().getHost().isPresent()
        ? sender.path().address().toString()
        : memberId;
  }

  private Logger log() {
    return actorContext.getSystem().log();
  }

  static String entityId(Integer nodePort, int id) {
    return String.format("%d-%d", nodePort, id);
  }

  public interface Command extends CborSerializable {}

  public static class ChangeValue implements Command {
    public final Id id;
    public final Value value;
    public final ActorRef<Command> replyTo;

    @JsonCreator
    public ChangeValue(Id id, Value value, ActorRef<Command> replyTo) {
      this.id = id;
      this.value = value;
      this.replyTo = replyTo;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]",
          getClass().getSimpleName(), id, value);
    }
  }

  public static class ChangeValueAck implements Command {
    public final String action;
    public final Id id;
    public final Value value;

    @JsonCreator
    public ChangeValueAck(String action, Id id, Value value) {
      this.action = action;
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %s]",
          getClass().getSimpleName(), action, id, value);
    }
  }

  public static class CreateCacheEntry implements Command {
    public final Id id;
    public final CachedItem<?> cachedItem;
    public final ActorRef<Command> replyTo;

    @JsonCreator
    public CreateCacheEntry(
        Id id,
        CachedItem<?> cachedItem,
        ActorRef<Command> replyTo) {
      this.id = id;
      this.cachedItem = cachedItem;
      this.replyTo = replyTo;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]",
          getClass().getSimpleName(), id);
    }
  }

  public static class CreateCacheEntryAck implements Command {
    public final String action;
    public final Id id;
    public final com.fasterxml.jackson.databind.JsonNode value;

    @JsonCreator
    public CreateCacheEntryAck(
        String action,
        Id id,
        com.fasterxml.jackson.databind.JsonNode value) {
      this.action = action;
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s, %s]",
          getClass().getSimpleName(), action, id, value);
    }
  }

  public static class GetValue implements Command {
    public final Id id;
    public final ActorRef<Command> replyTo;

    @JsonCreator
    public GetValue(Id id, ActorRef<Command> replyTo) {
      this.id = id;
      this.replyTo = replyTo;
    }
  }

  public static class GetValueAck implements Command {
    public final Id id;
    public final Value value;

    @JsonCreator
    public GetValueAck(Id id, Value value) {
      this.id = id;
      this.value = value;
    }
  }

  public static class GetValueAckNotFound implements Command {
    public final Id id;

    @JsonCreator
    public GetValueAckNotFound(Id id) {
      this.id = id;
    }
  }

  public enum Passivate implements Command {
    INSTANCE
  }

  private static class State {
    final Id id;
    Value value;

    State(Id id, Value value) {
      this.id = id;
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]",
          getClass().getSimpleName(), id, value);
    }
  }

  static class Id implements CborSerializable {
    public final String id;

    @JsonCreator
    public Id(String id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]",
          getClass().getSimpleName(), id);
    }
  }

  static class Value implements CborSerializable {
    public final Object value;

    @JsonCreator
    public Value(Object value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]",
          getClass().getSimpleName(), value);
    }
  }
}
