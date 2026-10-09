package cluster;

import java.math.BigDecimal;
import java.time.Duration;

import org.slf4j.Logger;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import cluster.EntityActor.Command;

class EntityCommandActor extends AbstractBehavior<Command> {
    private final ActorContext<Command> actorContext;
    private final ClusterSharding clusterSharding;
    private final int entitiesPerNode;
    private final Integer nodePort;

    static Behavior<Command> create() {
        return Behaviors.setup(actorContext -> Behaviors.withTimers(timer -> new EntityCommandActor(actorContext, timer)));
    }

    private EntityCommandActor(ActorContext<Command> actorContext, TimerScheduler<Command> timerScheduler) {

        super(actorContext);
        this.actorContext = actorContext;
        this.clusterSharding = ClusterSharding.get(actorContext.getSystem());
        this.entitiesPerNode = actorContext.getSystem().settings().config().getInt("entity-actor.entities-per-node");
        final var interval = Duration.parse(actorContext.getSystem().settings().config().getString("entity-actor.command-tick-interval-iso-8601"));
        timerScheduler.startSingleTimer(Tick.ticktock, interval);
        this.nodePort = actorContext.getSystem().address().getPort().orElse(-1);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder().onMessage(Tick.class, tick -> onTick()).onMessage(EntityActor.ChangeValueAck.class, this::onChangeValueAck).onMessage(EntityActor.CreateCacheEntryAck.class, this::onCreateCacheEntryAck).build();
    }

    private Behavior<Command> onTick() {
        log().info("Starting cache initialization");
        ClusterSharding sharding = ClusterSharding.get(actorContext.getSystem());
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < 100; i++) {
            int idValue = 1000 + i;
            TestItem testItem = new TestItem(BigDecimal.valueOf(idValue), "Ajay", String.valueOf(idValue));
            JsonNode jsonNode = mapper.valueToTree(testItem);
            CachedItem<TestItem> cachedItem = new CachedItem<>("TestItem", jsonNode);
            String entityId = EntityActor.entityId(nodePort, idValue);
            EntityActor.Id id = new EntityActor.Id(entityId);
            var entityRef = sharding.entityRefFor(EntityActor.entityTypeKey, entityId);
            entityRef.tell(new EntityActor.CreateCacheEntry(id, cachedItem, actorContext.getSelf()));
        }
        log().info("Finished cache initialization. entitiesPerNode={}", entitiesPerNode);
        return this;
    }

    private Behavior<Command> onChangeValueAck(EntityActor.ChangeValueAck ack) {
        log().info("{}", ack);
        return this;
    }

    private Behavior<Command> onCreateCacheEntryAck(EntityActor.CreateCacheEntryAck ack) {
        log().info("Cache Entry Created Successfully: {} -> {}", ack.id, ack.value);
        return this;
    }

    private Logger log() {
        return actorContext.getSystem().log();
    }

    enum Tick implements EntityActor.Command {
        ticktock
    }
}
