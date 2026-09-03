const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");


async function installPublicationProbe(page, {
  discardFirst = false,
  failRetry = false,
} = {}) {
  // Observe the real classic-script publication and boot call without loading
  // the module separately. Optionally discard the first publication to model a
  // local script that did not finish loading before app.js starts.
  await page.addInitScript(({ discardFirst, failRetry }) => {
    const events = [];
    let initializer;
    let publicationCount = 0;
    window.__punctRestoreStartupEvents = events;
    window.__punctRestoreRetryFailures = [];
    Object.defineProperty(window, "initPunctRestoreDialog", {
      configurable: true,
      get() {
        return initializer;
      },
      set(value) {
        publicationCount += 1;
        events.push({
          type: "published",
          source: document.currentScript?.getAttribute("src") || "",
          readyState: document.readyState,
          accepted: !(discardFirst && publicationCount === 1),
        });
        if (discardFirst && publicationCount === 1) {
          initializer = undefined;
          return;
        }
        initializer = function (...args) {
          events.push({
            type: "initialized",
            source: document.currentScript?.getAttribute("src") || "",
            readyState: document.readyState,
          });
          return value.apply(this, args);
        };
      },
    });

    if (failRetry) {
      const appendChild = Node.prototype.appendChild;
      Node.prototype.appendChild = function (node) {
        if (node instanceof HTMLScriptElement
            && node.dataset.bootRetry === "initPunctRestoreDialog") {
          window.__punctRestoreRetryFailures.push(node.src);
          queueMicrotask(() => node.onerror?.(new Event("error")));
          return node;
        }
        return appendChild.call(this, node);
      };
    }
  }, { discardFirst, failRetry });
}


async function openAndClosePunctuationDialog(page) {
  await page.locator('.tab[data-tab="health"]').click();
  await page.locator(
    '#panel-health .settings-subnav-btn[data-settings-view="library"]',
  ).click();
  await page.evaluate(() => {
    document.getElementById("health-library-transcripts").open = true;
    document.querySelector("#health-library-transcripts .health-advanced").open = true;
  });

  const button = page.locator("#btn-punct-restore");
  const backdrop = page.locator("#punct-restore-backdrop");
  await expect(button).toBeVisible();
  await expect(backdrop).toBeHidden();
  await button.click();
  await expect(backdrop).toBeVisible();
  await expect(page.locator("#punct-restore-channel option").first())
    .toHaveText("All channels");

  await page.locator("#punct-restore-close").click();
  await expect(backdrop).toBeHidden();
}


test("punctuation restore loads before boot and its Health action opens the dialog", async ({ page }) => {
  await installPublicationProbe(page);

  await loadApp(page);

  const startup = await page.evaluate(() => {
    const scripts = [...document.scripts]
      .map((script) => script.getAttribute("src") || "");
    return {
      scripts,
      events: window.__punctRestoreStartupEvents,
      issue: (window.YT?.bootIssues || []).find(
        (entry) => entry.name === "initPunctRestoreDialog"),
      initializerType: typeof window.initPunctRestoreDialog,
      bannerText: document.getElementById("boot-issue-banner")?.innerText || "",
    };
  });

  const punctScript = startup.scripts.findIndex((src) =>
    /^punctRestoreDialog\.js(?:\?|$)/.test(src));
  const appScript = startup.scripts.findIndex((src) =>
    /^app\.js(?:\?|$)/.test(src));
  expect(punctScript).toBeGreaterThanOrEqual(0);
  expect(appScript).toBeGreaterThan(punctScript);
  expect(startup.initializerType).toBe("function");
  expect(startup.events.map((event) => event.type))
    .toEqual(["published", "initialized"]);
  expect(startup.events[0].source).toMatch(/^punctRestoreDialog\.js(?:\?|$)/);
  expect(startup.issue).toBeUndefined();
  expect(startup.bannerText).not.toContain("initPunctRestoreDialog");

  await openAndClosePunctuationDialog(page);
});


test("a missing punctuation initializer is cache-busted, reloaded, and initialized", async ({ page }) => {
  await installPublicationProbe(page, { discardFirst: true });
  await loadApp(page);

  await page.waitForFunction(() =>
    window.__punctRestoreStartupEvents
      ?.some((event) => event.type === "initialized"));

  const recovered = await page.evaluate(() => {
    const retry = document.querySelector(
      'script[data-boot-retry="initPunctRestoreDialog"]');
    return {
      events: window.__punctRestoreStartupEvents,
      retryUrl: retry?.src || "",
      issueCount: (window.YT?.bootIssues || []).filter(
        (entry) => entry.name === "initPunctRestoreDialog").length,
      bannerText: document.getElementById("boot-issue-banner")?.innerText || "",
      initializerType: typeof window.initPunctRestoreDialog,
    };
  });

  expect(recovered.events.map((event) => event.type))
    .toEqual(["published", "published", "initialized"]);
  expect(recovered.events.map((event) => event.accepted))
    .toEqual([false, true, undefined]);
  expect(recovered.events[1].source).toContain("punctRestoreDialog.js");
  expect(recovered.initializerType).toBe("function");
  const retryUrl = new URL(recovered.retryUrl);
  expect(retryUrl.pathname).toMatch(/\/punctRestoreDialog\.js$/);
  expect(retryUrl.searchParams.get("v")).toBe("2");
  expect(retryUrl.searchParams.get("boot-retry")).toMatch(/^\d+$/);
  expect(recovered.issueCount).toBe(0);
  expect(recovered.bannerText).not.toContain("startup issue");

  await openAndClosePunctuationDialog(page);
});


test("a failed punctuation retry reports one specific startup issue", async ({ page }) => {
  await installPublicationProbe(page, {
    discardFirst: true,
    failRetry: true,
  });
  await loadApp(page);

  await page.waitForFunction(() =>
    (window.YT?.bootIssues || [])
      .some((entry) => entry.name === "initPunctRestoreDialog"));

  const failure = await page.evaluate(() => ({
    events: window.__punctRestoreStartupEvents,
    failedUrls: window.__punctRestoreRetryFailures,
    issues: (window.YT?.bootIssues || []).filter(
      (entry) => entry.name === "initPunctRestoreDialog"),
  }));

  expect(failure.events).toHaveLength(1);
  expect(failure.events[0]).toMatchObject({
    type: "published",
    accepted: false,
  });
  expect(failure.failedUrls).toHaveLength(1);
  const failedUrl = new URL(failure.failedUrls[0]);
  expect(failedUrl.pathname).toMatch(/\/punctRestoreDialog\.js$/);
  expect(failedUrl.searchParams.get("v")).toBe("2");
  expect(failedUrl.searchParams.get("boot-retry")).toMatch(/^\d+$/);
  expect(failure.issues).toHaveLength(1);
  expect(failure.issues[0].message).toBe(
    "punctRestoreDialog.js failed to load again; punctuation repair is unavailable for this session.",
  );

  const banner = page.locator("#boot-issue-banner");
  await expect(banner).toBeVisible();
  await expect(banner.locator("#boot-issue-summary"))
    .toHaveText("1 startup issue. Some features may not be available.");
});
