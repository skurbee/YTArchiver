/* Stable frontend event/state owner for backend bridge pushes. */
(function () {
  "use strict";

  window.YT = window.YT || {};
  const values = new Map();
  const listeners = new Map();

  function publish(topic, value) {
    topic = String(topic || "").trim();
    if (!topic) throw new TypeError("event topic is required");
    values.set(topic, value);
    for (const listener of Array.from(listeners.get(topic) || [])) {
      try { listener(value); } catch (error) {
        console.error(`YT event listener failed (${topic})`, error);
      }
    }
  }

  function subscribe(topic, listener, options) {
    topic = String(topic || "").trim();
    if (!topic || typeof listener !== "function") {
      throw new TypeError("event topic and listener are required");
    }
    let topicListeners = listeners.get(topic);
    if (!topicListeners) {
      topicListeners = new Set();
      listeners.set(topic, topicListeners);
    }
    topicListeners.add(listener);
    if (options?.replay !== false && values.has(topic)) {
      listener(values.get(topic));
    }
    return () => {
      topicListeners.delete(listener);
      if (!topicListeners.size) listeners.delete(topic);
    };
  }

  function snapshot(topic) {
    return values.get(String(topic || "").trim());
  }

  const controlListeners = new Map();
  const received = new Map();
  const accepting = new Map();

  function remember(key, revision) {
    received.set(key, Math.max(received.get(key) || 0, revision));
    if (received.size > 4096) received.delete(received.keys().next().value);
  }

  function onControl(kinds, listener) {
    const topics = Array.isArray(kinds) ? kinds : [kinds];
    for (const kind of topics) {
      if (!controlListeners.has(kind)) controlListeners.set(kind, new Set());
      controlListeners.get(kind).add(listener);
    }
    // Preserve direct DOM events for local callers; reliable deliveries below
    // invoke only registered consumers and never depend on log rendering.
    const legacy = event => {
      if (topics.includes(event.detail?.kind)) listener(event);
    };
    window.addEventListener("yt-control", legacy);
    return () => {
      window.removeEventListener("yt-control", legacy);
      for (const kind of topics) controlListeners.get(kind)?.delete(listener);
    };
  }

  window._appEventBatch = function (events) {
    const acknowledged = [];
    for (const event of Array.isArray(events) ? events : []) {
      if (!event || !Number.isSafeInteger(event.revision) || !event.key || !event.channel) continue;
      const key = `${event.channel}:${event.key}`;
      if ((received.get(key) || 0) >= event.revision) {
        acknowledged.push(event.revision);
        continue;
      }
      try {
        if (event.expires_at != null && Number(event.expires_at) * 1000 <= Date.now()) {
          acknowledged.push(event.revision);
          continue;
        }
        if (event.topic === "control") {
          if (accepting.get(key)?.revision === event.revision) continue;
          const handlers = [...(controlListeners.get(event.payload?.kind) || [])];
          if (!handlers.length) continue;
          const results = [];
          for (const handler of handlers) {
            try { results.push(handler({ detail: event.payload })); }
            catch (error) { results.push(Promise.reject(error)); }
          }
          if (results.some(result => result && typeof result.then === "function")) {
            // A Promise is still an in-flight receipt, not successful delivery.
            // Retries wait for it without reopening a dialog. A newer close
            // event can still supersede an older pending open on the same key.
            const receipt = { revision: event.revision };
            accepting.set(key, receipt);
            Promise.all(results).then(values => {
              if (values.every(value => value !== false)) remember(key, event.revision);
            }).catch(error => console.error("Control event will retry", error)).finally(() => {
              if (accepting.get(key) === receipt) accepting.delete(key);
            });
            continue;
          }
          if (results.some(result => result === false)) continue;
        } else if (event.topic === "processing") {
          if (typeof window._onProcessingEvent !== "function") continue;
          if (window._onProcessingEvent(event.payload) === false) continue;
        } else continue;
        remember(key, event.revision);
        acknowledged.push(event.revision);
      } catch (error) {
        console.error("Application event will retry", error);
      }
    }
    return acknowledged;
  };

  window.YT.eventState = Object.freeze({ publish, subscribe, snapshot, onControl });

  // This callback is a stable bridge endpoint. Consumers subscribe to the
  // named topic instead of repeatedly wrapping/replacing the global function.
  window.setQueueState = function (state) {
    publish("queue-state", state || { sync: {}, gpu: {} });
  };
})();
