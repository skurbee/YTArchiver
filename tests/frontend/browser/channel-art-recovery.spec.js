const { test, expect, loadApp } = require("./fixtures");

const ORIGIN = "http://127.0.0.1:39991";
const PNG = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=", "base64");
const art = name => `${ORIGIN}/file/${name}.jpg?t=fixture-token`;
const channel = (name, extra = {}) => ({ folder: name, name, n_vids: 7,
  size: "70 MB", subscriber_count: 1200, ...extra });
const cardFor = (page, name) => page.locator(`#channel-grid .channel-card[data-channel-name="${name}"]`);

async function render(page, channels) {
  await page.evaluate(channels => {
    window._browseState.channels = channels;
    window._browseState.channelsReady = true;
    window.renderChannelGrid(channels, () => {});
  }, channels);
}

async function decoded(image) {
  await expect.poll(() => image.evaluate(img => img.complete && img.naturalWidth > 0)).toBe(true);
  await expect(image).toBeVisible();
}

test("unreadable small art falls back once to original files and preserves channel details", async ({ page }) => {
  const requests = [];
  await page.route(`${ORIGIN}/**`, async route => {
    const url = new URL(route.request().url());
    requests.push(url);
    await route.fulfill({ status: 200, contentType: "image/jpeg",
      body: url.pathname.includes("small") ? Buffer.from("truncated image") : PNG });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Recovered art", {
    banner_url: art("banner-small"), banner_fallback_url: art("banner-original"),
    avatar_url: art("avatar-small"), avatar_fallback_url: art("avatar-original"),
  })]);
  const card = cardFor(page, "Recovered art");
  await decoded(card.locator(".channel-card-bg"));
  await decoded(card.locator(".channel-avatar"));
  await expect(card.locator(".channel-card-bg")).toHaveAttribute("src", /banner-original.*art_retry=/);
  await expect(card.locator(".channel-avatar")).toHaveAttribute("src", /avatar-original.*art_retry=/);
  expect(requests.every(url => url.searchParams.get("t") === "fixture-token")).toBe(true);
  await expect(card).toContainText("1.2K subscribers");
  await page.evaluate(() => window._refreshChannelCardSummary("Recovered art", { n_vids: 8 }));
  await expect(card).toContainText("8 videos");
  await decoded(card.locator(".channel-card-bg"));
});

test("a stalled visible image reaches its original fallback without waiting for the hung response", async ({ page }) => {
  const pending = [];
  await page.clock.install();
  await page.route(`${ORIGIN}/**`, async route => {
    if (route.request().url().includes("small")) { pending.push(route); return; }
    await route.fulfill({ status: 200, contentType: "image/png", body: PNG });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Stalled art", {
    banner_url: art("hung-small"), banner_fallback_url: art("healthy-original"),
  })]);
  await expect.poll(() => pending.length).toBeGreaterThan(0);
  await expect(cardFor(page, "Stalled art").locator(".channel-card-bg")).toHaveAttribute("loading", "eager");
  await page.clock.fastForward(5100);
  await decoded(cardFor(page, "Stalled art").locator(".channel-card-bg"));
  await expect(cardFor(page, "Stalled art").locator(".channel-card-bg"))
    .toHaveAttribute("src", /healthy-original/);
  await Promise.allSettled(pending.map(route => route.abort()));
});

test("transient localhost failures retry the same authenticated URL only once when no original is supplied", async ({ page }) => {
  const requests = [];
  await page.route(`${ORIGIN}/**`, async route => {
    const url = new URL(route.request().url());
    requests.push(url);
    await route.fulfill(url.searchParams.has("art_retry")
      ? { status: 200, contentType: "image/png", body: PNG }
      : { status: 503, body: "temporarily unavailable" });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Transient art", { banner_url: art("transient") })]);
  await decoded(cardFor(page, "Transient art").locator(".channel-card-bg"));
  expect(requests.every(url => url.pathname === "/file/transient.jpg"
    && url.searchParams.get("t") === "fixture-token")).toBe(true);
  // At most two consumers (visible image and prefetch), each with one retry.
  expect(requests.length).toBeLessThanOrEqual(4);
});

test("permanent banner and avatar failures settle on a letter without repeated retry traffic", async ({ page }) => {
  await page.clock.install();
  let requests = 0;
  await page.route(`${ORIGIN}/**`, async route => {
    requests++;
    await route.fulfill({ status: 500, body: "unreadable" });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Unavailable art", {
    banner_url: art("bad-banner"), avatar_url: art("bad-avatar"),
  })]);
  const card = cardFor(page, "Unavailable art");
  await expect(card.locator(".channel-letter")).toHaveText("U");
  await expect(card.locator("img")).toHaveCount(0);
  await page.clock.fastForward(30000);
  const settledCount = requests;
  await page.clock.fastForward(30000);
  expect(requests).toBe(settledCount);
  expect(requests).toBeLessThanOrEqual(8);
  await expect(card).toContainText("7 videos");
});

test("a failed banner retains the healthy avatar and does not fetch an unrelated fallback origin", async ({ page }) => {
  let unrelated = 0;
  await page.route("http://unrelated.fixture/**", async route => { unrelated++; await route.abort(); });
  await page.route(`${ORIGIN}/**`, async route => {
    await route.fulfill(route.request().url().includes("avatar")
      ? { status: 200, contentType: "image/png", body: PNG }
      : { status: 500, body: "unreadable" });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Avatar remains", { banner_url: art("bad-banner"),
    banner_fallback_url: "http://unrelated.fixture/private.jpg", avatar_url: art("good-avatar") })]);
  const card = cardFor(page, "Avatar remains");
  await expect(card.locator(".channel-card-bg")).toHaveCount(0);
  await decoded(card.locator(".channel-avatar"));
  await expect(card.locator(".channel-letter")).toHaveCount(0);
  expect(unrelated).toBe(0);
});

test("hung prefetch slots expire, stay within forty cards, and do not block later visible art", async ({ page }) => {
  await page.clock.install();
  const pending = [], requests = [];
  await page.route(`${ORIGIN}/**`, async route => {
    const url = new URL(route.request().url());
    requests.push(url);
    if (/\/prefetch-[01]\.jpg$/.test(url.pathname)) { pending.push(route); return; }
    await route.fulfill({ status: 200, contentType: "image/png", body: PNG });
  });
  await loadApp(page);
  const rows = Array.from({ length: 45 }, (_, index) => channel(`Card ${index}`, {
    banner_url: art(`prefetch-${index}`),
  }));
  // Keep Browse hidden: only the two-slot prefetch queue may issue requests.
  await render(page, rows);
  await expect.poll(() => requests.length).toBe(2);
  await page.clock.fastForward(5100);
  await expect.poll(() => requests.filter(url => url.searchParams.has("art_retry")).length).toBe(2);
  await page.clock.fastForward(5100);
  await page.clock.runFor(600);
  await expect.poll(() => requests.some(url => url.pathname === "/file/prefetch-39.jpg")).toBe(true);
  expect(requests.some(url => url.pathname === "/file/prefetch-40.jpg")).toBe(false);
  expect(requests.filter(url => /\/prefetch-[01]\.jpg$/.test(url.pathname))).toHaveLength(4);
  await page.locator('.tab[data-tab="browse"]').click();
  await cardFor(page, "Card 44").scrollIntoViewIfNeeded();
  await decoded(cardFor(page, "Card 44").locator(".channel-card-bg"));
  await Promise.allSettled(pending.map(route => route.abort()));
});

test("rerender cancels old deadlines and late responses cannot alter the replacement card", async ({ page }) => {
  await page.clock.install();
  const pending = [], requests = [];
  await page.route(`${ORIGIN}/**`, async route => {
    const url = new URL(route.request().url());
    requests.push(url);
    if (url.pathname.includes("old")) { pending.push(route); return; }
    await route.fulfill({ status: 200, contentType: "image/png", body: PNG });
  });
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await render(page, [channel("Replaced channel", { banner_url: art("old-hung") })]);
  await expect.poll(() => pending.length).toBeGreaterThan(0);
  await render(page, [channel("New channel", { banner_url: art("new-healthy") })]);
  const card = cardFor(page, "New channel");
  await decoded(card.locator(".channel-card-bg"));
  await page.clock.fastForward(20000);
  await Promise.allSettled(pending.map(route => route.fulfill({ status: 500, body: "late error" })));
  await decoded(card.locator(".channel-card-bg"));
  await expect(cardFor(page, "Replaced channel")).toHaveCount(0);
  expect(requests.filter(url => url.pathname.includes("old")
    && url.searchParams.has("art_retry"))).toHaveLength(0);
  await expect(card.locator(".channel-letter")).toHaveCount(0);
});
