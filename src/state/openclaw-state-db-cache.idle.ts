import { SQLITE_IDLE_HANDLE_TTL_MS } from "../infra/sqlite-handle-lifecycle.js";
import { runInSqliteMaintenanceContext } from "../infra/sqlite-wal.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import type { StateDatabaseLifecycle } from "./openclaw-state-db-cache.types.js";
import type {
  OpenClawStateDatabase,
  OpenClawStateDatabaseCloseOptions,
} from "./openclaw-state-db-contract.js";

const log = createSubsystemLogger("state/db");

/** Schedule native retirement against the canonical cache's handles and borrow pins. */
export function createStateDatabaseIdleRetirement(
  { cachedDatabases, retainedDatabaseHandles, idleTimers, borrowers }: StateDatabaseLifecycle,
  retire: (
    database: OpenClawStateDatabase,
    retireAdmission: boolean,
    options: OpenClawStateDatabaseCloseOptions,
  ) => void,
): (database: OpenClawStateDatabase) => void {
  const touch = (database: OpenClawStateDatabase): void => {
    if (
      !(cachedDatabases.get(database.path) === database && database.db.isOpen) &&
      retainedDatabaseHandles.get(database.db) !== database
    ) {
      return;
    }
    const previous = idleTimers.get(database.db);
    if (previous) {
      previous.refresh();
      return;
    }
    const timer = runInSqliteMaintenanceContext(() =>
      setTimeout(() => {
        idleTimers.delete(database.db);
        try {
          if (
            database.db.isOpen &&
            (database.db.isTransaction || borrowers.get(database.db)?.references.size)
          ) {
            touch(database);
            return;
          }
          // Native expiry does not revoke independently active worker admission.
          retire(database, false, { busyTimeoutMs: 0, checkpointMode: "PASSIVE" });
        } catch (error) {
          log.warn("Idle shared-state database cleanup failed", { path: database.path, error });
          touch(database);
        }
      }, SQLITE_IDLE_HANDLE_TTL_MS),
    );
    timer.unref();
    idleTimers.set(database.db, timer);
  };
  return touch;
}
