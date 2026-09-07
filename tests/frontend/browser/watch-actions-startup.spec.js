const { test, expect, APP_URL, installBridgeStub, loadApp } = require("./fixtures");

async function missFirstPublication(page, failRetry = false) {
  await page.addInitScript(({ failRetry }) => {
    let initializer;
    window.__watchPublications = 0;
    window.__watchInitializations = 0;
    window.__watchRetryFailures = [];
    Object.defineProperty(window, "initWatchActions", {
      configurable: true,
      get() { return initializer; },
      set(value) {
        window.__watchPublications += 1;
        // Model the observed absent export at boot; the recovery must load
        // the actual script again and wire its real controls.
        if (window.__watchPublications === 1) return;
        initializer = function (...args) {
          window.__watchInitializations += 1;
          return value.apply(this, args);
        };
      },
    });
    if (failRetry) {
      const appendChild = Node.prototype.appendChild;
      Node.prototype.appendChild = function (node) {
        if (node instanceof HTMLScriptElement && node.dataset.bootRetry === "initWatchActions") {
          window.__watchRetryFailures.push(node.src);
          queueMicrotask(() => node.onerror?.(new Event("error")));
          return node;
        }
        return appendChild.call(this, node);
      };
    }
  }, { failRetry });
}

test("normal Watch startup loads once without a recovery request", async ({ page }) => {
  await loadApp(page);
  expect(await page.evaluate(() => ({
    initialized: window._watchActionsInited,
    retries: document.querySelectorAll('script[data-boot-retry="initWatchActions"]').length,
    issues: window.YT.bootIssues.filter(issue => issue.name === "initWatchActions"),
  }))).toEqual({ initialized: true, retries: 0, issues: [] });
});

test("a missing Watch script export reloads once and wires the real controls", async ({ page }) => {
  await missFirstPublication(page);
  await loadApp(page);
  const result = await page.evaluate(() => {
    const button = document.getElementById("btn-tx-nonspeech");
    const transcript = document.getElementById("watch-transcript");
    button.click();
    const hidden = transcript.classList.contains("hide-nonspeech");
    button.click();
    return {
      publications: window.__watchPublications,
      initializations: window.__watchInitializations,
      retry: document.querySelector('script[data-boot-retry="initWatchActions"]').src,
      hidden, restored: !transcript.classList.contains("hide-nonspeech"),
      issues: window.YT.bootIssues.filter(issue => issue.name === "initWatchActions"),
    };
  });
  expect(result.publications).toBe(2);
  expect(result.initializations).toBe(1);
  expect(new URL(result.retry).searchParams.get("boot-retry")).toMatch(/^\d+$/);
  expect(result.hidden).toBe(true);
  expect(result.restored).toBe(true);
  expect(result.issues).toEqual([]);
});

test("a persistently missing Watch module reports one specific failure", async ({ page }) => {
  await missFirstPublication(page, true);
  await page.addInitScript(installBridgeStub);
  // loadApp intentionally requires working Watch controls; this test instead
  // waits for the observable failure while the other app modules still boot.
  await page.goto(APP_URL, { waitUntil: "load" });
  await page.waitForFunction(() => window.YT?.bootIssues?.some(issue => issue.name === "initWatchActions"));
  const failure = await page.evaluate(() => ({
    retries: window.__watchRetryFailures,
    issues: window.YT.bootIssues.filter(issue => issue.name === "initWatchActions"),
    initialized: !!window._watchActionsInited,
    queuesAvailable: typeof window.renderQueues === "function",
  }));
  expect(failure.retries).toHaveLength(1);
  expect(failure.issues).toHaveLength(1);
  expect(failure.issues[0].message).toBe(
    "watchActions.js failed to load again; the Watch toolbar is unavailable for this session.",
  );
  expect(failure.initialized).toBe(false);
  expect(failure.queuesAvailable).toBe(true);
});
