const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("bridge event state has one stable owner and isolated subscribers", async ({ page }) => {
  await loadApp(page);

  const result = await page.evaluate(() => {
    const stableEndpoint = window.setQueueState;
    const seen = [];
    const unsubscribeBroken = window.YT.eventState.subscribe(
      "queue-state",
      () => { throw new Error("injected listener failure"); },
      { replay: false },
    );
    const unsubscribeHealthy = window.YT.eventState.subscribe(
      "queue-state",
      (value) => seen.push(value.sync.running),
      { replay: false },
    );
    window.setQueueState({
      sync: { running: true, paused: false },
      gpu: { running: false, paused: false },
    });
    window.renderQueues({ sync: [], gpu: [], sync_count: 4, gpu_count: 2 });
    const snapshot = window.YT.eventState.snapshot("queue-state");
    const payload = window.YT.eventState.snapshot("queue-payload");
    unsubscribeBroken();
    unsubscribeHealthy();
    return {
      endpointStayedStable: stableEndpoint === window.setQueueState,
      seen,
      running: snapshot.sync.running,
      syncCount: payload.sync_count,
      gpuCount: payload.gpu_count,
    };
  });

  expect(result).toEqual({
    endpointStayedStable: true,
    seen: [true],
    running: true,
    syncCount: 4,
    gpuCount: 2,
  });
});
