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

  window.YT.eventState = Object.freeze({ publish, subscribe, snapshot });

  // This callback is a stable bridge endpoint. Consumers subscribe to the
  // named topic instead of repeatedly wrapping/replacing the global function.
  window.setQueueState = function (state) {
    publish("queue-state", state || { sync: {}, gpu: {} });
  };
})();
