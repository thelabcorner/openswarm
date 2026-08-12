import { SQLiteStore } from "./sqlite-store.js";
import { ChunkDbStore } from "./chunkdb-store.js";

/** Per-entity copy counts reported by migrateSwarmDb. */
export interface MigrationCounts {
  swarms: number;
  members: number;
  tasks: number;
  messages: number;
  blackboard: number;
  beliefs: number;
  annotations: number;
  claims: number;
  subscriptions: number;
  permissions: number;
  deliverables: number;
  contracts: number;
  events: number;
  dependencies: number;
}

/**
 * Migrate a SQLite swarm database (srcPath) into a fresh chunkDB database
 * (destPath) entirely through the PUBLIC store API on both sides — the same
 * read methods the plugin uses at runtime and the same write methods, so the
 * migrated data is byte-identical in shape to what a live chunkdb store would
 * produce. Entity ids are preserved exactly.
 *
 * srcPath is opened with SQLiteStore (its ready() auto-migrates legacy DBs),
 * destPath with ChunkDbStore. Both are closed before returning.
 */
export async function migrateSwarmDb(
  srcPath: string,
  destPath: string,
): Promise<MigrationCounts> {
  const src = new SQLiteStore(srcPath);
  const dest = new ChunkDbStore(destPath);
  await src.ready();
  await dest.ready();

  const counts: MigrationCounts = {
    swarms: 0,
    members: 0,
    tasks: 0,
    messages: 0,
    blackboard: 0,
    beliefs: 0,
    annotations: 0,
    claims: 0,
    subscriptions: 0,
    permissions: 0,
    deliverables: 0,
    contracts: 0,
    events: 0,
    dependencies: 0,
  };

  try {
    const swarmIds = await src.listAllMemberSwarmIds();
    for (const swarmId of swarmIds) {
      const swarm = await src.getSwarm(swarmId);
      if (!swarm) continue;
      await dest.insertSwarm(swarm);
      counts.swarms++;

      for (const member of await src.listMembers(swarmId)) {
        await dest.insertMember(member);
        counts.members++;
      }

      for (const task of await src.listTasks(swarmId)) {
        await dest.insertTask(task);
        counts.tasks++;
      }

      for (const dep of await src.listTaskDependencies(swarmId)) {
        await dest.insertTaskDependency(dep.taskId, dep.dependsOnTaskId);
        counts.dependencies++;
      }

      // Messages are copied in one batch so they land in compressed chunks.
      const messages = await src.listMessagesBySwarm(swarmId, 1_000_000);
      if (messages.length > 0) {
        await dest.insertMessages(messages);
        counts.messages += messages.length;
      }

      for (const entry of await src.listBlackboardEntries(swarmId)) {
        await dest.insertBlackboard(entry);
        counts.blackboard++;
      }

      for (const belief of await src.listBeliefs(swarmId, { activeOnly: false })) {
        await dest.insertBelief(belief);
        counts.beliefs++;
      }

      for (const annotation of await src.listAnnotations(swarmId, { activeOnly: false })) {
        await dest.insertAnnotation(annotation);
        counts.annotations++;
      }

      for (const claim of await src.listPathClaims(swarmId, 0)) {
        await dest.insertPathClaim(claim);
        counts.claims++;
      }

      for (const sub of await src.listSubscriptions(swarmId)) {
        await dest.addSubscription(sub.swarmId, sub.memberId, sub.pattern);
        counts.subscriptions++;
      }

      for (const permission of await src.listPendingPermissions(swarmId)) {
        await dest.insertPendingPermission(permission);
        counts.permissions++;
      }

      for (const deliverable of await src.listDeliverables(swarmId, { limit: 1_000_000 })) {
        await dest.insertDeliverable(deliverable);
        counts.deliverables++;
      }

      for (const contract of await src.listContracts(swarmId)) {
        await dest.insertContract(contract);
        counts.contracts++;
      }

      for (const event of await src.listEvents(swarmId, { limit: 1_000_000 })) {
        // Preserve the original autoincrement ids by pinning them on the
        // NewSwarmEvent (ChunkDbStore honors a runtime `id` and bumps its
        // per-swarm counter past it).
        await dest.insertEvent(event);
        counts.events++;
      }
    }
    return counts;
  } finally {
    await src.close();
    await dest.close();
  }
}
