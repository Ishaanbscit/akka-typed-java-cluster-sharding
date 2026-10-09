package cluster;

import java.math.BigDecimal;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;

/**
 * Small standalone cache-sharding demo.
 *
 * Run with a cluster node port, for example:
 *   java -cp ... cluster.CacheMain 2551
 *
 * For the normal full dashboard application use cluster.Main instead.
 */
public class CacheMain {
  public static void main(String[] args) {
    String port = args.length == 0 ? "2551" : args[0];

    ActorSystem<Void> system =
        ActorSystem.create(
            Behaviors.empty(),
            "CacheSystem",
            com.typesafe.config.ConfigFactory
                .parseString(
                    "akka.remote.artery.canonical.port = " + port)
                .withFallback(
                    com.typesafe.config.ConfigFactory.load()));

    ClusterSharding sharding = ClusterSharding.get(system);

    // This demo reuses the real EntityActor and therefore needs
    // the HTTP actor reference in the full application. For a quick
    // standalone example, initialize the same actor type with null.
    sharding.init(
        Entity.of(
            EntityActor.entityTypeKey,
            entityContext ->
                EntityActor.create(entityContext.getEntityId(), null))
        .withStopMessage(EntityActor.Passivate.INSTANCE));

    for (int i = 0; i < 10; i++) {
      int id = 1000 + i;
      TestItem item =
          new TestItem(
              BigDecimal.valueOf(id),
              "Ajay",
              String.valueOf(id));

      com.fasterxml.jackson.databind.ObjectMapper mapper =
          new com.fasterxml.jackson.databind.ObjectMapper();

      CachedItem<TestItem> cachedItem =
          new CachedItem<>("TestItem", mapper.valueToTree(item));

      String entityId = EntityActor.entityId(
          Integer.valueOf(port), id);

      sharding
          .entityRefFor(EntityActor.entityTypeKey, entityId)
          .tell(
              new EntityActor.CreateCacheEntry(
                  new EntityActor.Id(entityId),
                  cachedItem,
                  system.ignoreRef()));
    }

    system.log().info("CacheMain started on port {}", port);
  }
}
