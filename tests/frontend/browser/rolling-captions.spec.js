const { test, expect } = require("@playwright/test");
const { loadApp } = require("./fixtures");

const FLOW_WORDS = [
  "Amber", "birches", "crossing", "distant", "evergreen", "forests",
  "gather", "hushed", "islands", "joining", "kindred", "landscapes",
  "morning", "northward", "orchards", "passing", "quietly", "rivers",
  "silver", "trails", "under", "valleys", "wandering", "xylophones",
  "yellow", "zephyrs", "brighten", "cloudless", "daylight", "endlessly",
];

function segment(words, start = 0, step = 0.4, duration = 0.28) {
  const timed = words.map((w, i) => ({
    w, s: start + i * step, e: start + i * step + duration,
  }));
  return { s: timed[0].s, e: timed.at(-1).e, text: words.join(" "), words: timed };
}

async function settleFrames(page) {
  await page.evaluate(() => new Promise(resolve => {
    requestAnimationFrame(() => requestAnimationFrame(resolve));
  }));
}

async function renderFixture(page, transcript, options = {}) {
  await page.evaluate(({ transcript, options }) => {
    window.showView("watch");
    const video = document.getElementById("watch-video");
    // No media URL, network request, decoder, or real playback is involved.
    window.__rollingCaptionTime = options.time ?? 0.02;
    Object.defineProperty(video, "currentTime", {
      configurable: true,
      get: () => window.__rollingCaptionTime,
      set: value => { window.__rollingCaptionTime = Number(value); },
    });
    Object.defineProperty(video, "paused", { configurable: true, get: () => true });
    video.hidden = false;
    video.style.aspectRatio = "16 / 9";
    document.getElementById("watch-video-placeholder").hidden = true;
    const wrap = video.closest(".watch-video-wrap");
    wrap.style.width = `${options.width ?? 560}px`;
    wrap.style.maxWidth = "100%";
    window.renderWatchView({
      video_id: options.videoId || "caption-fixture",
      title: options.title || "Caption fixture",
      channel: "Example",
    }, transcript, null, {
      skipVideoReload: true,
      ...(options.state === "loading" ? { transcriptLoading: true } : {}),
      ...(options.state === "error" ? { transcriptError: "Temporary read failure" } : {}),
    });
  }, { transcript, options });
  await settleFrames(page);
}

async function openFixture(page, transcript, options = {}) {
  await loadApp(page);
  await page.locator('.tab[data-tab="browse"]').click();
  await renderFixture(page, transcript, options);
  await page.locator("#watch-cap-size").selectOption(options.size || "medium");
  await page.locator("#watch-cap-mode").selectOption("default");
  await settleFrames(page);
}

async function setTime(page, time, seek = false) {
  await page.evaluate(({ time, seek }) => {
    const video = document.getElementById("watch-video");
    window.__rollingCaptionTime = time;
    if (seek) video.dispatchEvent(new Event("seeking"));
    video.dispatchEvent(new Event("timeupdate"));
    if (seek) video.dispatchEvent(new Event("seeked"));
  }, { time, seek });
  await settleFrames(page);
}

async function caption(page) {
  return page.locator("#watch-cap-ovl").evaluate(overlay => ({
    shown: overlay.classList.contains("show") && getComputedStyle(overlay).display !== "none",
    lines: Array.from(overlay.querySelectorAll(".cap-ovl-line"), line => line.textContent.trim()),
    cells: Array.from(overlay.children, cell => cell.textContent.trim()),
    boxes: Array.from(overlay.querySelectorAll(".cap-ovl-line"), line => {
      const rect = line.getBoundingClientRect();
      const range = document.createRange();
      range.selectNodeContents(line);
      const text = range.getBoundingClientRect();
      return {
        x: rect.x, y: rect.y, width: rect.width, height: rect.height,
        textLeft: text.left, textRight: text.right, textHeight: text.height,
      };
    }),
    width: overlay.getBoundingClientRect().width,
    left: overlay.getBoundingClientRect().left,
    right: overlay.getBoundingClientRect().right,
    height: overlay.getBoundingClientRect().height,
  }));
}

function assertSpokenSuffix(snapshot, spoken) {
  expect(snapshot.shown).toBe(true);
  expect(snapshot.lines).toHaveLength(2);
  const tokens = snapshot.lines.join(" ").trim().split(/\s+/);
  expect(tokens).toEqual(spoken.slice(-tokens.length));
  expect(snapshot.lines[1].split(/\s+/).at(-1)).toBe(spoken.at(-1));
  for (const [index, box] of snapshot.boxes.entries()) {
    expect(box.height).toBeGreaterThan(0);
    if (!snapshot.lines[index]) continue;
    expect(box.textLeft).toBeGreaterThanOrEqual(snapshot.left - 1);
    expect(box.textRight).toBeLessThanOrEqual(snapshot.right + 1);
  }
}

async function findRollingBoundary(page, speech) {
  let previous = await caption(page);
  for (let i = 1; i < speech.words.length; i++) {
    await setTime(page, speech.words[i].s + 0.02);
    const current = await caption(page);
    if (previous.lines[0] && current.lines[0] !== previous.lines[0]) {
      return { index: i, before: previous, after: current };
    }
    previous = current;
  }
  return null;
}

test("YT style reveals only spoken words in two steady rows", async ({ page }) => {
  const speech = segment(["Alpha", "bravo", "charlie"], 1);
  await openFixture(page, [speech], { time: 0.5, width: 640 });
  expect((await caption(page)).shown).toBe(false);
  await expect(page.locator('#watch-cap-mode option[value="default"]')).toHaveText("YT Style");

  let first;
  for (let i = 0; i < speech.words.length; i++) {
    await setTime(page, speech.words[i].s + 0.02);
    const current = await caption(page);
    expect(current.shown).toBe(true);
    expect(current.lines).toEqual(["", speech.words.slice(0, i + 1).map(w => w.w).join(" ")]);
    if (!first) first = current;
    expect(current.height).toBeCloseTo(first.height, 1);
    expect(current.boxes[1].x).toBeCloseTo(first.boxes[1].x, 1);
    expect(current.boxes[1].y).toBeCloseTo(first.boxes[1].y, 1);
    expect(current.boxes[1].width).toBeCloseTo(first.boxes[1].width, 1);
  }
});

test("completed rows roll upward at measured width and seeks reconstruct the same text", async ({ page }) => {
  const speech = segment(FLOW_WORDS, 1);
  await openFixture(page, [speech]);
  const snapshots = [];
  let rolls = 0;
  for (let i = 0; i < speech.words.length; i++) {
    await setTime(page, speech.words[i].s + 0.02);
    const current = await caption(page);
    assertSpokenSuffix(current, FLOW_WORDS.slice(0, i + 1));
    if (i > 0) {
      const previous = snapshots[i - 1];
      if (current.lines[0] !== previous.lines[0]) {
        rolls++;
        expect(current.lines).toEqual([previous.lines[1], FLOW_WORDS[i]]);
      } else {
        expect(current.lines[1]).toBe(`${previous.lines[1]} ${FLOW_WORDS[i]}`);
      }
      expect(current.height).toBeCloseTo(previous.height, 1);
    }
    snapshots.push(current);
  }
  expect(rolls).toBeGreaterThanOrEqual(2);

  for (const index of [3, 23, 11, 29, 0]) {
    await setTime(page, speech.words[index].s + 0.02, true);
    expect((await caption(page)).lines).toEqual(snapshots[index].lines);
  }
  await setTime(page, 0, true);
  expect((await caption(page)).shown).toBe(false);
});

test("silence clears the overlay and resets context across segment boundaries", async ({ page }) => {
  const before = segment(FLOW_WORDS.slice(0, 13));
  const lastEnd = before.words.at(-1).e;
  const after = segment(["Reset", "begins"], lastEnd + 0.78);
  await openFixture(page, [before, after], { time: before.words.at(-1).s + 0.02 });
  const previous = await caption(page);
  expect(previous.lines[0]).not.toBe("");

  await setTime(page, lastEnd + 0.65);
  expect((await caption(page)).lines).toEqual(previous.lines);
  expect((await caption(page)).shown).toBe(true);
  await setTime(page, lastEnd + 0.72);
  expect((await caption(page)).shown).toBe(false);
  await setTime(page, after.s + 0.02);
  expect((await caption(page)).lines).toEqual(["", "Reset"]);
  await setTime(page, after.words.at(-1).e + 0.65);
  expect((await caption(page)).shown).toBe(true);
  await setTime(page, after.words.at(-1).e + 0.72);
  expect((await caption(page)).shown).toBe(false);

  await setTime(page, before.words.at(-1).s + 0.02, true);
  expect((await caption(page)).lines).toEqual(previous.lines);
  await setTime(page, after.s + 0.02, true);
  expect((await caption(page)).lines).toEqual(["", "Reset"]);
});

test("short gaps retain context but a long segment cannot extend the final-word hold", async ({ page }) => {
  const first = segment(["Still", "together"]);
  const second = segment(["across", "segments"], first.e + 0.65);
  second.e += 20; // A coarse segment end must not keep stale words visible.
  await openFixture(page, [first, second], { time: second.s + 0.02, width: 640 });
  expect((await caption(page)).lines).toEqual(["", "Still together across"]);
  await setTime(page, second.words.at(-1).e + 0.65);
  expect((await caption(page)).shown).toBe(true);
  await setTime(page, second.words.at(-1).e + 0.72);
  expect((await caption(page)).shown).toBe(false);
});

test("an unbroken long word fits the overlay without extra rows", async ({ page }) => {
  const longWord = "Supercalifragilisticexpialidocious".repeat(3);
  const speech = segment([longWord, "next"]);
  await openFixture(page, [speech], { width: 360, size: "large" });
  const current = await caption(page);
  assertSpokenSuffix(current, [longWord]);
  expect(current.lines).toEqual(["", longWord]);
  expect(current.boxes[1].textHeight).toBeLessThanOrEqual(current.boxes[1].height + 1);
  await setTime(page, speech.words[1].s + 0.02);
  const rolled = await caption(page);
  assertSpokenSuffix(rolled, [longWord, "next"]);
  expect(rolled.lines).toEqual([longWord, "next"]);
});

test("paused size, mode, resize, and window fullscreen changes rebuild the current rows", async ({ page }) => {
  const speech = segment(FLOW_WORDS);
  const index = 18;
  const time = speech.words[index].s + 0.02;
  await openFixture(page, [speech], { time, size: "small", width: 560 });
  const small = await caption(page);
  await page.locator("#watch-cap-size").selectOption("large");
  await settleFrames(page);
  const large = await caption(page);
  assertSpokenSuffix(large, FLOW_WORDS.slice(0, index + 1));
  expect(large.lines).not.toEqual(small.lines);
  expect(large.boxes[1].height).toBeGreaterThan(small.boxes[1].height);

  await page.locator("#watch-cap-mode").selectOption("single");
  expect((await caption(page)).cells).toEqual(["", FLOW_WORDS[index], ""]);
  await page.locator("#watch-cap-mode").selectOption("phrase3");
  expect((await caption(page)).cells).toEqual(FLOW_WORDS.slice(index - 1, index + 2));
  await page.locator("#watch-cap-mode").selectOption("default");
  expect((await caption(page)).lines).toEqual(large.lines);

  await page.evaluate(() => {
    document.querySelector(".watch-video-wrap").style.width = "360px";
    window.dispatchEvent(new Event("resize"));
  });
  await settleFrames(page);
  const narrow = await caption(page);
  assertSpokenSuffix(narrow, FLOW_WORDS.slice(0, index + 1));
  expect(narrow.width).toBeLessThan(large.width);
  expect(narrow.lines).toEqual(large.lines);
  expect(narrow.boxes[1].height).toBeLessThan(large.boxes[1].height);

  await page.locator("#watch-video-stage").hover();
  await page.locator("#watch-fs-btn").click();
  await expect(page.locator("#watch-video-stage")).toHaveClass(/cssfs/);
  await settleFrames(page);
  const fullscreen = await caption(page);
  assertSpokenSuffix(fullscreen, FLOW_WORDS.slice(0, index + 1));
  expect(fullscreen.width).toBeGreaterThan(narrow.width);
  expect(fullscreen.lines).toEqual(narrow.lines);
  expect(fullscreen.boxes[1].height).toBeGreaterThan(narrow.boxes[1].height);
  await page.keyboard.press("Escape");
  await expect(page.locator("#watch-video-stage")).not.toHaveClass(/cssfs/);
  await settleFrames(page);
  expect((await caption(page)).lines).toEqual(narrow.lines);
  expect(await page.locator("#watch-video").evaluate(video => video.currentTime)).toBe(time);
});

test("playing rolls both completed rows upward inside a clipped two-row viewport", async ({ page }) => {
  const speech = segment(FLOW_WORDS, 0, 0.08, 0.06);
  await openFixture(page, [speech]);
  // Discover a second wrap from measured output; font/platform differences
  // must not turn a particular word number into the test's contract.
  const boundary = await findRollingBoundary(page, speech);
  expect(boundary).not.toBeNull();
  await setTime(page, speech.words[boundary.index + 1].s + 0.02);
  const afterNextWord = await caption(page);
  expect(afterNextWord.lines[0]).toBe(boundary.after.lines[0]);
  await setTime(page, speech.words[boundary.index - 1].s + 0.02, true);

  const motion = await page.evaluate(async ({ time, nextTime, older, completed }) => {
    const frame = () => new Promise(resolve => requestAnimationFrame(resolve));
    const video = document.getElementById("watch-video");
    const overlay = document.getElementById("watch-cap-ovl");
    Object.defineProperty(video, "paused", { configurable: true, get: () => false });
    // A normal animation frame initiates the roll without a forced seek.
    window.__rollingCaptionTime = time;
    await frame();
    await frame();
    const animations = overlay.getAnimations({ subtree: true })
      .filter(animation => animation.playState === "running");
    video.dispatchEvent(new Event("timeupdate"));
    window.__rollingCaptionTime = nextTime;
    await frame();
    const continued = animations.length > 0
      && animations.every(animation => animation.playState === "running");
    const rows = () => Array.from(overlay.querySelectorAll(".cap-ovl-line"));
    const findY = text => rows().find(row => row.textContent.trim() === text)
      ?.getBoundingClientRect().y;
    let clip = overlay.querySelector(".cap-ovl-cur");
    while (clip && clip !== video.parentElement
           && !["hidden", "clip"].includes(getComputedStyle(clip).overflowY)) {
      clip = clip.parentElement;
    }
    const clipRect = clip?.getBoundingClientRect();
    const overflowY = clip ? getComputedStyle(clip).overflowY : "";
    // Inspect real browser animation output at deterministic progress points,
    // rather than relying on the machine completing a timer within 250ms.
    const sample = async progress => {
      for (const animation of animations) {
        animation.pause();
        animation.currentTime = Number(animation.effect.getTiming().duration) * progress;
      }
      await frame();
      return { olderY: findY(older), completedY: findY(completed) };
    };
    const early = await sample(0.15);
    const late = await sample(0.8);
    for (const animation of animations) animation.finish();
    await Promise.allSettled(animations.map(animation => animation.finished));
    await frame();
    return {
      count: animations.length, continued, early, late, overflowY, clipHeight: clipRect?.height,
    };
  }, {
    time: speech.words[boundary.index].s + 0.02,
    nextTime: speech.words[boundary.index + 1].s + 0.02,
    older: boundary.before.lines[0], completed: boundary.before.lines[1],
  });
  expect(motion.count).toBeGreaterThan(0);
  expect(motion.continued).toBe(true);
  expect(motion.late.olderY).toBeLessThan(motion.early.olderY);
  expect(motion.late.completedY).toBeLessThan(motion.early.completedY);
  expect(["hidden", "clip"]).toContain(motion.overflowY);
  const rowHeight = boundary.before.boxes.reduce((total, row) => total + row.height, 0);
  expect(motion.clipHeight).toBeGreaterThanOrEqual(rowHeight - 2);
  expect(motion.clipHeight).toBeLessThanOrEqual(rowHeight + 8);
  await settleFrames(page);
  expect((await caption(page)).lines).toEqual(afterNextWord.lines);
});

test("accelerated playback finishes each narrow-player roll before the next line arrives", async ({ page }) => {
  const words = [
    "ExtraordinarilyLongFirstWord", "CounterclockwiseSecondWord",
    "IntercontinentalThirdWord", "MisunderstandingFourthWord",
    "CharacteristicallyFifthWord", "UnquestionablySixthWord",
  ];
  const speech = segment(words, 0, 0.34, 0.3);
  await openFixture(page, [speech], { width: 300, size: "large", time: speech.words[1].s + 0.02 });
  expect((await caption(page)).lines).toEqual(words.slice(0, 2));
  await page.locator("#watch-speed").selectOption("2");

  const transitions = await page.evaluate(async speech => {
    const frame = () => new Promise(resolve => requestAnimationFrame(resolve));
    const video = document.getElementById("watch-video");
    const overlay = document.getElementById("watch-cap-ovl");
    Object.defineProperty(video, "paused", { configurable: true, get: () => false });
    const results = [];
    for (const index of [2, 3]) {
      const time = speech.words[index].s + 0.02;
      window.__rollingCaptionTime = time;
      await frame();
      await frame();
      const animations = overlay.getAnimations({ subtree: true })
        .filter(animation => animation.playState === "running");
      const durations = animations.map(animation => Number(animation.effect.getTiming().duration));
      const sample = async progress => {
        animations.forEach((animation, i) => {
          animation.pause();
          animation.currentTime = durations[i] * progress;
        });
        await frame();
        const row = Array.from(overlay.querySelectorAll(".cap-ovl-line"))
          .find(element => element.textContent.trim() === speech.words[index - 1].w);
        return row?.getBoundingClientRect().y;
      };
      const earlyY = await sample(0.15);
      const lateY = await sample(0.8);
      animations.forEach(animation => animation.play());
      await Promise.allSettled(animations.map(animation => animation.finished));
      await frame();
      results.push({
        durations, earlyY, lateY,
        timeUntilNextLine: (speech.words[index + 1].s - time) * 1000 / video.playbackRate,
        remaining: overlay.getAnimations({ subtree: true }).length,
        lines: Array.from(overlay.querySelectorAll(".cap-ovl-line"), row => row.textContent.trim()),
      });
    }
    return results;
  }, speech);
  expect(transitions).toHaveLength(2);
  for (const [offset, transition] of transitions.entries()) {
    expect(transition.durations.length).toBeGreaterThan(0);
    expect(transition.timeUntilNextLine).toBeLessThan(250);
    for (const duration of transition.durations) {
      expect(duration).toBeGreaterThan(0);
      expect(duration).toBeLessThan(transition.timeUntilNextLine);
    }
    expect(transition.lateY).toBeLessThan(transition.earlyY);
    expect(transition.remaining).toBe(0);
    expect(transition.lines).toEqual(words.slice(offset + 1, offset + 3));
  }
});

test("Off and word modes cancel an in-flight roll without leaving moving caption rows", async ({ page }) => {
  const speech = segment(FLOW_WORDS, 0, 0.08, 0.06);
  await openFixture(page, [speech]);
  const boundary = await findRollingBoundary(page, speech);
  expect(boundary).not.toBeNull();
  for (const mode of ["off", "single", "phrase3"]) {
    await page.evaluate(() => {
      Object.defineProperty(document.getElementById("watch-video"), "paused", {
        configurable: true, get: () => true,
      });
    });
    await page.locator("#watch-cap-size").selectOption("medium");
    await page.locator("#watch-cap-mode").selectOption("default");
    await setTime(page, speech.words[boundary.index - 1].s + 0.02, true);
    const transition = await page.evaluate(async ({ time, mode }) => {
      const frame = () => new Promise(resolve => requestAnimationFrame(resolve));
      const video = document.getElementById("watch-video");
      const overlay = document.getElementById("watch-cap-ovl");
      Object.defineProperty(video, "paused", { configurable: true, get: () => false });
      window.__rollingCaptionTime = time;
      await frame();
      await frame();
      const started = overlay.getAnimations({ subtree: true })
        .filter(animation => animation.playState === "running").length;
      // Dispatch through the real toolbar handler while the animation is
      // active, avoiding pointer/actionability timing exceeding its duration.
      const select = document.getElementById(mode === "off" ? "watch-cap-size" : "watch-cap-mode");
      select.value = mode;
      select.dispatchEvent(new Event("change", { bubbles: true }));
      await frame();
      await frame();
      return { started, remaining: overlay.getAnimations({ subtree: true }).length };
    }, { time: speech.words[boundary.index].s + 0.02, mode });
    expect(transition.started).toBeGreaterThan(0);
    expect(transition.remaining).toBe(0);
    const current = await caption(page);
    if (mode === "off") expect(current.shown).toBe(false);
    else {
      expect(current.shown).toBe(true);
      expect(current.lines).toEqual([]);
      expect(current.cells).toEqual(mode === "single"
        ? ["", FLOW_WORDS[boundary.index], ""]
        : FLOW_WORDS.slice(boundary.index - 1, boundary.index + 2));
    }
  }
});

for (const state of ["loading", "error", "empty"]) {
  test(`a new video's ${state} state cannot revive the previous rolling caption`, async ({ page }) => {
    await openFixture(page, [segment(FLOW_WORDS)], { time: 5.62 });
    expect((await caption(page)).shown).toBe(true);
    await renderFixture(page, [], { state, videoId: "second-fixture", title: "Second fixture" });
    await page.locator("#watch-cap-size").selectOption("large");
    await page.locator("#watch-cap-bg").selectOption("outline");
    await page.evaluate(() => window.dispatchEvent(new Event("resize")));
    await setTime(page, 5.62, true);
    expect((await caption(page)).shown).toBe(false);
    await expect(page.locator("#watch-title")).toHaveText("Second fixture");

    await renderFixture(page, [segment(["Fresh", "caption"])], {
      videoId: "second-fixture", title: "Second fixture",
    });
    expect((await caption(page)).lines).toEqual(["", "Fresh"]);
    expect((await caption(page)).shown).toBe(true);
  });
}

test("Off stays hidden during seeks and preference changes, and enabling uses the current time", async ({ page }) => {
  const speech = segment(FLOW_WORDS);
  await openFixture(page, [speech]);
  await page.locator("#watch-cap-size").selectOption("off");
  await expect(page.locator("#watch-overlay-extras")).not.toBeVisible();
  await setTime(page, speech.words[18].s + 0.02, true);
  await page.evaluate(() => {
    window.setCaptionPref("bg", "none");
    window.dispatchEvent(new Event("resize"));
  });
  await settleFrames(page);
  expect((await caption(page)).shown).toBe(false);
  await page.locator("#watch-cap-size").selectOption("medium");
  await settleFrames(page);
  assertSpokenSuffix(await caption(page), FLOW_WORDS.slice(0, 19));
});

for (const mode of ["single", "phrase3", "default"]) {
  test(`caption size and background survive reload while saved ${mode} starts as YT Style`, async ({ page }) => {
    const speech = segment(["Earlier", "current", "later"]);
    await openFixture(page, [speech], { time: speech.words[1].s + 0.02 });
    await page.locator("#watch-cap-size").selectOption("large");
    await page.locator("#watch-cap-bg").selectOption("outline");
    await page.locator("#watch-cap-mode").selectOption(mode);
    expect(await page.evaluate(() => ({
      size: localStorage.getItem("ytarchiver_caption_size"),
      bg: localStorage.getItem("ytarchiver_caption_bg"),
      mode: localStorage.getItem("ytarchiver_caption_mode"),
    }))).toEqual({ size: "large", bg: "outline", mode });
    await expect.poll(() => page.evaluate(mode => window.__bridgeCallsFor("settings_save")
      .some(call => call.args[0]?.caption_overlay_mode === mode), mode)).toBe(true);

    await page.reload({ waitUntil: "load" });
    await page.waitForFunction(() => window._watchActionsInited === true);
    await page.locator('.tab[data-tab="browse"]').click();
    await renderFixture(page, [speech], { time: speech.words[1].s + 0.02, width: 640 });
    await expect(page.locator("#watch-cap-size")).toHaveValue("large");
    await expect(page.locator("#watch-cap-bg")).toHaveValue("outline");
    await expect(page.locator("#watch-cap-mode")).toHaveValue("default");
    const current = await caption(page);
    expect(current.shown).toBe(true);
    expect(current.lines).toEqual(["", "Earlier current"]);
  });
}
