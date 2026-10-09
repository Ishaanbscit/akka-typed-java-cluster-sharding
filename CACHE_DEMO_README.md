# Cache / DPCache additions

This project is based on the uploaded `akka-typed-java-cluster-sharding` project.

Added/updated:
- `CachedItem.java` - JSON wrapper for cached objects.
- `TestItem.java` - sample cached POJO.
- `DataCacheCommands.java` - base/type-level and entry-level cache commands.
- `EntityActor.java` - supports `CreateCacheEntry`, `ChangeValue`, `GetValue`, and passivation.
- `EntityCommandActor.java` - initializes 100 `TestItem` cache entities.
- `CacheMain.java` - small standalone sharding demo.
- Existing dashboard, cluster management, scripts, and original project files are retained.

Important:
- The cache entity id used by the demo is `<nodePort>-<id>`.
- The DPCache-style `CreateDataCacheEntry.entityId()` is
  `<tenantCode>_<terminalCode>_<cacheEntryClassName>_<id>`.
- `Main` remains the recommended entry point for the full dashboard/cluster demo.

Build:
```bash
mvn clean package
```

Run the full cluster demo:
```bash
java -jar target/akka-typed-java-cluster-sharding-1.0-SNAPSHOT-allinone.jar 2551
java -jar target/akka-typed-java-cluster-sharding-1.0-SNAPSHOT-allinone.jar 2552
java -jar target/akka-typed-java-cluster-sharding-1.0-SNAPSHOT-allinone.jar 2553
```
