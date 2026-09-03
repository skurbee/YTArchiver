const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

test("removed videos keep only their badge and remain normal clickable cards", async ({ page }) => {
  await loadApp(page);
  await page.evaluate(() => {
    const host = document.createElement("div");
    host.dataset.removedVisualHost = "1";
    host.style.cssText = [
      "position:fixed", "left:20px", "top:20px", "z-index:20000",
      "display:grid", "grid-template-columns:280px 280px", "gap:12px",
    ].join(";");
    document.body.appendChild(host);

    const common = {
      channel: "Removed-card fixture",
      filepath: "C:\\FixtureArchive\\removed-card.mp4",
      thumbnail_url:
        "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==",
    };
    window.__removedCardClicks = [];
    const normal = window._buildVideoCard({
      ...common,
      video_id: "normal00001",
      title: "Normal video",
      removed_from_yt: false,
    }, (video) => window.__removedCardClicks.push(video.video_id));
    const removed = window._buildVideoCard({
      ...common,
      video_id: "removed0001",
      title: "Removed video",
      removed_from_yt: true,
    }, (video) => window.__removedCardClicks.push(video.video_id));
    normal.dataset.removedVisualFixture = "normal";
    removed.dataset.removedVisualFixture = "removed";
    host.append(normal, removed);
  });

  const normal = page.locator('[data-removed-visual-fixture="normal"]');
  const removed = page.locator('[data-removed-visual-fixture="removed"]');
  const normalThumb = normal.locator(".video-thumb");
  const removedThumb = removed.locator(".video-thumb");

  await expect(normal).not.toHaveClass(/\bvideo-card-removed\b/);
  await expect(normal).not.toHaveAttribute("data-removed-from-yt");
  await expect(normal.locator(".video-removed-badge")).toHaveCount(0);
  await expect(removed).toHaveClass(/\bvideo-card-removed\b/);
  await expect(removed).toHaveAttribute("data-removed-from-yt", "1");
  await expect(removed.locator(".video-removed-badge"))
    .toHaveText("✗ Removed from YT");

  await expect(normalThumb).toHaveCSS("filter", "none");
  await expect(removedThumb).toHaveCSS("filter", "none");
  const normalTitleColor = await normal.locator(".video-card-title")
    .evaluate((element) => getComputedStyle(element).color);
  await expect(removed.locator(".video-card-title"))
    .toHaveCSS("color", normalTitleColor);

  await removed.hover();
  await expect(removedThumb).toHaveCSS("filter", "none");
  await expect(removed.locator(".video-card-title"))
    .toHaveCSS("color", normalTitleColor);

  await removed.click();
  await expect.poll(() => page.evaluate(() => window.__removedCardClicks))
    .toEqual(["removed0001"]);
});
