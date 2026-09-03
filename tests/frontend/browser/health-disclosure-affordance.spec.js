const fs = require("node:fs");
const path = require("node:path");
const { test, expect } = require("@playwright/test");

const REPO_ROOT = path.resolve(__dirname, "..", "..", "..");
const WEB_ROOT = path.join(REPO_ROOT, "web");
const HEALTH_PARTIAL = fs.readFileSync(
  path.join(WEB_ROOT, "partials", "tab-health.html"), "utf8");
const SETTINGS_PARTIAL = fs.readFileSync(
  path.join(WEB_ROOT, "partials", "tab-settings.html"), "utf8");

const DISCLOSURES = [
  ["#health-library-metadata > summary", "Channel information"],
  ["#health-library-index .health-advanced > summary", "Advanced index repair"],
  ["#health-library-archive > summary", "Archive-file repairs"],
  ["#health-library-archive .health-advanced > summary", "Advanced repairs"],
  ["#health-library-transcripts > summary", "Transcript repairs"],
  ["#health-library-transcripts .health-advanced > summary", "Legacy transcript repairs"],
  ["#settings-background-checks > summary", "Background checks"],
  ["#settings-downloader-updates > summary", "Downloader updates"],
  ["#settings-about-troubleshooting > summary", "About & troubleshooting"],
];

async function mountPanels(page) {
  await page.setContent(
    "<!doctype html><html><head><meta charset=\"utf-8\"></head><body>" +
      HEALTH_PARTIAL + SETTINGS_PARTIAL + "</body></html>");
  for (const stylesheet of ["styles.css", "styles-settings.css"]) {
    await page.addStyleTag({ path: path.join(WEB_ROOT, stylesheet) });
  }
  await page.evaluate(() => {
    document.getElementById("panel-health").classList.add("active");
    document.getElementById("settings-view-overview").hidden = true;
    document.getElementById("settings-view-library").hidden = false;
    document.getElementById("settings-view-backups").hidden = true;
    document.getElementById("panel-settings").classList.add("active");
  });
}

async function makeSummaryVisible(summary) {
  await summary.evaluate((element) => {
    const ownDetails = element.parentElement;
    ownDetails.open = false;
    let ancestor = ownDetails.parentElement?.closest("details");
    while (ancestor) {
      ancestor.open = true;
      ancestor = ancestor.parentElement?.closest("details");
    }
  });
  await summary.scrollIntoViewIfNeeded();
}

async function chevronStyle(summary) {
  return summary.evaluate((element) => {
    const style = getComputedStyle(element, "::after");
    const rect = element.getBoundingClientRect();
    return {
      content: style.content,
      display: style.display,
      opacity: Number(style.opacity),
      color: style.color,
      transform: style.transform,
      borderTopWidth: Number.parseFloat(style.borderTopWidth),
      borderRightWidth: Number.parseFloat(style.borderRightWidth),
      borderBottomWidth: Number.parseFloat(style.borderBottomWidth),
      borderLeftWidth: Number.parseFloat(style.borderLeftWidth),
      chevronWidth: Number.parseFloat(style.width),
      chevronHeight: Number.parseFloat(style.height),
      headerWidth: rect.width,
      headerHeight: rect.height,
    };
  });
}

function visualState(style) {
  return `${style.content}|${style.transform}`;
}

async function accessibleExpanded(cdpSession, namePrefix) {
  const tree = await cdpSession.send("Accessibility.getFullAXTree");
  const disclosure = tree.nodes.find((node) => (
    node.role?.value === "DisclosureTriangle" &&
    String(node.name?.value || "").startsWith(namePrefix)
  ));
  return disclosure?.properties?.find((property) => property.name === "expanded")
    ?.value?.value;
}

test("every Health and Settings disclosure has a visible, stateful chevron", async ({ page }) => {
  await page.setViewportSize({ width: 1100, height: 800 });
  await mountPanels(page);
  const cdpSession = await page.context().newCDPSession(page);

  for (const [selector, accessibleName] of DISCLOSURES) {
    const summary = page.locator(selector);
    await expect(summary, selector).toHaveCount(1);
    await makeSummaryVisible(summary);
    await expect(summary, selector).toBeVisible();

    const closed = await chevronStyle(summary);
    expect(closed.display, `${selector} arrow must be rendered`).not.toBe("none");
    expect(closed.opacity, `${selector} arrow must not be transparent`).toBeGreaterThan(0.2);
    expect(closed.color, `${selector} arrow must not be transparent`).not.toBe("rgba(0, 0, 0, 0)");
    expect(closed.chevronWidth, `${selector} arrow must have visible width`).toBeGreaterThanOrEqual(5);
    expect(closed.chevronHeight, `${selector} arrow must have visible height`).toBeGreaterThanOrEqual(5);
    expect(closed.borderRightWidth, `${selector} needs the right stroke of a chevron`).toBeGreaterThanOrEqual(1);
    expect(closed.borderBottomWidth, `${selector} needs the bottom stroke of a chevron`).toBeGreaterThanOrEqual(1);
    expect(closed.borderTopWidth, `${selector} should form a chevron, not a box`).toBe(0);
    expect(closed.borderLeftWidth, `${selector} should form a chevron, not a box`).toBe(0);
    expect(closed.headerWidth, `${selector} header must have a visible box`).toBeGreaterThan(20);
    expect(closed.headerHeight, `${selector} header must have a visible box`).toBeGreaterThan(20);

    expect(await accessibleExpanded(cdpSession, accessibleName),
      `${selector} must expose its native collapsed state`).toBe(false);

    await summary.click();
    await expect(summary.locator("xpath=.."), selector).toHaveAttribute("open", "");
    expect(await accessibleExpanded(cdpSession, accessibleName),
      `${selector} must expose its native expanded state`).toBe(true);
    await expect.poll(async () => visualState(await chevronStyle(summary)), {
      message: `${selector} arrow must visibly change when expanded`,
    }).not.toBe(visualState(closed));
  }
});

test("Health disclosure headers work with both Enter and Space", async ({ page }) => {
  await mountPanels(page);
  const cdpSession = await page.context().newCDPSession(page);
  const summary = page.locator("#health-library-metadata > summary");
  const details = page.locator("#health-library-metadata");

  await summary.focus();
  await expect(summary).toBeFocused();
  await summary.press("Enter");
  await expect(details).toHaveAttribute("open", "");
  expect(await accessibleExpanded(cdpSession, "Channel information")).toBe(true);

  await summary.press("Space");
  await expect(details).not.toHaveAttribute("open", "");
  expect(await accessibleExpanded(cdpSession, "Channel information")).toBe(false);
  await expect(summary).toBeFocused();
});

test("ordinary Health and Settings section headings are not styled as disclosures", async ({ page }) => {
  await mountPanels(page);

  const ordinaryHeadings = page.locator(
    "#health-library-index > .health-library-heading, " +
    "#panel-settings .settings-section-heading");
  await expect(ordinaryHeadings).toHaveCount(4);

  const semantics = await ordinaryHeadings.evaluateAll((headings) => headings.map((heading) => ({
    tag: heading.tagName,
    expanded: heading.getAttribute("aria-expanded"),
    summaryChildren: heading.querySelectorAll("summary").length,
    pseudoContent: getComputedStyle(heading, "::after").content,
  })));
  for (const heading of semantics) {
    expect(heading.tag).not.toBe("SUMMARY");
    expect(heading.expanded).toBeNull();
    expect(heading.summaryChildren).toBe(0);
    expect(["none", "normal", "\"\""]).toContain(heading.pseudoContent);
  }
});
