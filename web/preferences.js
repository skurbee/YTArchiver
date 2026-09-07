/* One owner for preference reads and writes. Native formats remain unchanged. */
(function () {
  "use strict";
  const YT = window.YT;
  let snapshot = null;
  let confirmed = {};
  let hasLoaded = false;
  let loading = null;
  let writes = Promise.resolve();
  let writeRevision = 0;
  const revisions = new Map();
  const acknowledged = new Map();
  const pending = new Map();

  function copy(value) { return structuredClone(value); }
  function revision(key) { return revisions.get(key) || 0; }

  function load({ refresh = false } = {}) {
    if (loading) return loading;
    if (hasLoaded && !refresh) return Promise.resolve(copy(snapshot));
    const started = new Map(revisions);
    const startedAcknowledged = new Map(acknowledged);
    const startedPending = new Set(pending.keys());
    const changed = key => revision(key) !== (started.get(key) || 0)
      || startedPending.has(key) || pending.has(key);
    loading = (async () => {
      const result = await YT.api.settings_load();
      const next = { ...result };
      for (const [key, value] of Object.entries(snapshot || {})) {
        if (changed(key)) {
          next[key] = value;
        }
      }
      for (const [key, value] of Object.entries(result)) {
        // A first read can establish the rollback baseline while a save is
        // still pending. It must never replace a newer acknowledged write.
        if (acknowledged.get(key) === startedAcknowledged.get(key)) {
          confirmed[key] = value;
        }
      }
      snapshot = next;
      hasLoaded = true;
      return copy(snapshot);
    })().finally(() => { loading = null; });
    return loading;
  }

  function save(updates) {
    if (!updates || typeof updates !== "object" || Array.isArray(updates)) {
      return Promise.reject(new TypeError("Preference updates must be an object."));
    }
    const values = copy(updates);
    writeRevision++;
    const own = new Map();
    for (const [key, value] of Object.entries(values)) {
      const next = revision(key) + 1;
      revisions.set(key, next);
      own.set(key, next);
      pending.set(key, next);
      snapshot = { ...snapshot, [key]: value };
    }
    const operation = writes.then(async () => {
      const result = await YT.api.settings_save(values);
      YT.bridge.requireReply(result, "Settings save");
      if (result.ok !== true) throw new Error("Settings save was not acknowledged.");
      Object.assign(confirmed, values);
      for (const [key, version] of own) acknowledged.set(key, version);
      return result;
    });
    writes = operation.catch(() => {});
    return operation.catch(error => {
      for (const [key, version] of own) {
        if (revision(key) !== version) continue;
        if (Object.hasOwn(confirmed, key)) snapshot[key] = confirmed[key];
        else delete snapshot[key];
      }
      throw error;
    }).finally(() => {
      writeRevision++;
      for (const [key, version] of own) {
        if (pending.get(key) === version) pending.delete(key);
      }
    });
  }

  // Independent controls can protect local interaction from a delayed first read.
  // Hydration never writes; callers explicitly save after user input.
  function hydrate(apply, keys, { signal } = {}) {
    const started = new Map(keys.map(key => [key, revision(key)]));
    let attempt = null;
    let retryRequested = false;
    const retry = () => {
      if (attempt) { retryRequested = true; return; }
      run().catch(() => {});
    };
    const run = () => {
      attempt = load().then(settings => {
        if (!signal?.aborted && keys.every(key => revision(key) === started.get(key))) apply(settings);
        window.removeEventListener("pywebviewready", retry);
        retryRequested = false;
      }).finally(() => {
        attempt = null;
        if (retryRequested && !signal?.aborted) {
          retryRequested = false;
          queueMicrotask(retry);
        }
      });
      return attempt;
    };
    window.addEventListener("pywebviewready", retry, { signal });
    return YT.bridge.ready.then(() => attempt || run());
  }

  YT.preferences = Object.freeze({ load, save, hydrate, revision,
    writeRevision: () => writeRevision,
    isSaving: () => pending.size > 0,
    snapshot: () => snapshot ? copy(snapshot) : null,
  });
})();
