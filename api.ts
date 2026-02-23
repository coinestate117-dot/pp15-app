import { db } from "~/server/db";
import {
  getAuth,
  setRealtimeStore,
  upload,
  getBaseUrl,
  inviteUser,
} from "~/server/actions";
// ===== Supabase Import =====
import { createClient } from "@supabase/supabase-js";

type SupaClient = ReturnType<typeof createClient>;

// --- Crash diagnostics ---
// Cloudflare 1101 typically means the worker threw an exception.
// These handlers help surface the actual error in runtime logs.
try {
  const p: any = typeof process !== "undefined" ? (process as any) : null;
  if (p?.on && !p.__pp15CrashHandlersInstalled) {
    p.__pp15CrashHandlersInstalled = true;
    p.on("uncaughtException", (err: any) => {
      const msg = err?.message ? String(err.message) : "(no message)";
      const name = err?.name ? String(err.name) : "Error";
      console.error("[crash] uncaughtException", { name, msg });
    });
    p.on("unhandledRejection", (reason: any) => {
      const msg = reason?.message
        ? String(reason.message)
        : typeof reason === "string"
          ? reason
          : "(no message)";
      console.error("[crash] unhandledRejection", { msg });
    });
  }
} catch {}

try {
  console.log("[boot] api.ts loaded", {
    hasSupabaseUrl: !!(process.env.SUPABASE_URL || "").trim(),
    hasSupabaseAnonKey: !!(process.env.SUPABASE_ANON_KEY || "").trim(),
  });
} catch {}

let _supabase: SupaClient | null = null;

function _fetchWithTimeout(input: RequestInfo | URL, init?: RequestInit) {
  const timeoutMs = 8_000;
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  const mergedInit: RequestInit = {
    ...(init ?? {}),
    signal: controller.signal,
  };

  return fetch(input as any, mergedInit).finally(() => clearTimeout(t));
}

function getSupabaseClient(): SupaClient | null {
  const url = (process.env.SUPABASE_URL || "").trim();
  const anonKey = (process.env.SUPABASE_ANON_KEY || "").trim();
  if (!url || !anonKey) return null;
  if (!_supabase) {
    _supabase = createClient(url, anonKey, {
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        detectSessionInUrl: false,
      },
      global: {
        fetch: _fetchWithTimeout as any,
      },
    });
  }
  return _supabase;
}

function requireSupabaseClient(): SupaClient {
  const s = getSupabaseClient();
  if (!s) {
    throw new Error("Supabase is not configured.");
  }
  return s;
}
// ===== Bonding Curve Limits =====
export const MAX_SUPPLY = 400000;

// ===== Bonding Curve (m²) =====
// ===== Bonding Curve: Single Price Lookup =====
export async function getPriceUsdBySupply(
  supplyIndex: number,
): Promise<number | null> {
  const supabase = requireSupabaseClient() as any;
  const { data, error } = await supabase
    .from("bonding_curve")
    .select("price_usd")
    .eq("supply_index", supplyIndex)
    .single();

  if (error) {
    console.error("getPriceUsdBySupply error:", error.message);
    return null;
  }

  return data?.price_usd ?? null;
}

export async function loadCurrentSupply(): Promise<number> {
  const supabase = requireSupabaseClient() as any;
  const { data, error } = await supabase
    .from("market_state")
    .select("current_supply")
    .eq("id", 1)
    .single();

  if (error) throw error;
  return data?.current_supply ?? 0;
}

export async function loadBondingCurve(fromSupply: number, points = 200) {
  const supabase = requireSupabaseClient() as any;
  const { data, error } = await supabase.rpc("get_curve_series", {
    p_from_supply: fromSupply,
    p_points: points,
  });

  if (error) throw error;
  return data;
}

export async function getSupabasePublicConfig() {
  const url = process.env.SUPABASE_URL || "";
  const anonKey = process.env.SUPABASE_ANON_KEY || "";
  // Return safe defaults so the app can still run in guest mode even if Supabase isn't configured.
  return { url, anonKey };
}

export async function getServerStatus() {
  // This endpoint should never crash — it exists so the UI can detect outages and show a friendly fallback.
  const now = new Date().toISOString();

  const supabaseConfigured = !!(
    (process.env.SUPABASE_URL || "").trim() &&
    (process.env.SUPABASE_ANON_KEY || "").trim()
  );

  try {
    // Minimal DB touch to validate Prisma + DB connectivity.
    // Add a timeout so a slow DB doesn't make the whole app feel broken.
    const dbOk = await Promise.race<boolean>([
      (async () => {
        try {
          await db.market.findFirst({ select: { id: true } });
          return true;
        } catch {
          return false;
        }
      })(),
      new Promise<boolean>((resolve) =>
        setTimeout(() => resolve(false), 3_000),
      ),
    ]);

    if (!dbOk) {
      return {
        ok: false as const,
        now,
        supabaseConfigured,
        message: "Database unreachable",
      };
    }

    return {
      ok: true as const,
      now,
      supabaseConfigured,
    };
  } catch (e: any) {
    const message = e?.message ? String(e.message) : "Server unavailable";
    // Keep noise down, but still log for diagnostics.
    console.log("[status] getServerStatus failed", { message });
    return {
      ok: false as const,
      now,
      supabaseConfigured,
      message,
    };
  }
}

export async function sendPasswordRecoveryEmail(input: { email: string }) {
  const url = process.env.SUPABASE_URL || "";
  const anonKey = process.env.SUPABASE_ANON_KEY || "";

  if (!url || !anonKey) {
    throw new Error("Supabase is not configured.");
  }

  const email = (input?.email || "").trim();
  if (!email) {
    throw new Error("Enter your email first.");
  }

  // Only the email link should redirect (not this button).
  const redirectTo = "https://reset.pp15.one";

  const safeEmail = (() => {
    try {
      const m = email.match(/^(.)(.*)(@.*)$/);
      if (!m) return "(redacted)";
      return `${m[1]}***${m[3]}`;
    } catch {
      return "(redacted)";
    }
  })();

  console.log("[auth] sendPasswordRecoveryEmail start", {
    email: safeEmail,
    redirectTo,
    hasUrl: !!url,
    hasAnonKey: !!anonKey,
    hasServiceRoleKey: !!process.env.SUPABASE_SERVICE_ROLE_KEY,
  });

  try {
    // Use the Auth REST endpoint directly so we can surface detailed error responses.
    // Note: `redirect_to` must be a query param for /recover.
    const base = url.replace(/\/+$/, "");
    const endpoint = `${base}/auth/v1/recover?redirect_to=${encodeURIComponent(redirectTo)}`;

    // Use service role if available (server-side only). Keep headers consistent.
    const authHeaderKey = process.env.SUPABASE_SERVICE_ROLE_KEY || anonKey;

    const resp = await _fetchWithTimeout(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: authHeaderKey,
        Authorization: `Bearer ${authHeaderKey}`,
      },
      body: JSON.stringify({
        email,
      }),
    });

    const contentType = (resp.headers.get("content-type") || "").toLowerCase();
    let payload: any = null;
    let rawText = "";

    try {
      if (contentType.includes("application/json")) {
        payload = await resp.json();
      } else {
        rawText = await resp.text();
      }
    } catch {
      payload = null;
      rawText = "";
    }

    if (!resp.ok) {
      const msg =
        payload?.msg ||
        payload?.message ||
        payload?.error_description ||
        payload?.error ||
        (rawText ? rawText.slice(0, 300) : "") ||
        `Supabase error (${resp.status})`;

      console.log("[auth] sendPasswordRecoveryEmail error", {
        email: safeEmail,
        status: resp.status,
        message: msg,
        contentType,
        payloadMessage: payload?.message || "",
        payloadMsg: payload?.msg || "",
        payloadError: payload?.error || "",
        payloadErrorDesc: payload?.error_description || "",
      });

      throw new Error(`Supabase (${resp.status}): ${msg}`);
    }

    console.log("[auth] sendPasswordRecoveryEmail success", {
      email: safeEmail,
      status: resp.status,
    });

    return {
      ok: true,
      message: "Password reset email sent. Check your inbox.",
    };
  } catch (e: any) {
    const msg = e?.message || "Error sending recovery email";
    console.log("[auth] sendPasswordRecoveryEmail exception", {
      email: safeEmail,
      message: msg,
    });
    throw new Error(msg);
  }
}

// ===== Beispiel Funktionen =====

// Tabelle dynamisch lesen
export async function fetchFromSupabase(
  table: string,
): Promise<{ data: any[]; error: string | null }> {
  const supabase = requireSupabaseClient();
  const { data, error } = await supabase.from(table).select("*");

  return {
    data: (data as any[]) || [],
    error: error ? String(error.message || error) : null,
  };
}

// Tabelle dynamisch schreiben
export async function insertToSupabase(
  table: string,
  payload: any,
): Promise<{ data: any[]; error: string | null }> {
  const supabase = requireSupabaseClient();
  const { data, error } = await supabase
    .from(table)
    .insert(payload)
    .select("*");

  return {
    data: (data as any[]) || [],
    error: error ? String(error.message || error) : null,
  };
}

type NotificationTopic =
  | "TOPUP"
  | "BUY"
  | "SELL"
  | "SECONDARY_BUY"
  | "SECONDARY_SOLD"
  | "COMMISSION_IN"
  | "COMMISSION_OUT";

function _getUserNotificationTopics(): NotificationTopic[] {
  // E-Mails sind komplett deaktiviert.
  return [];
}

export async function _emailNotificationsCronHandler() {
  // E-Mail-Versand ist komplett deaktiviert.
  // Der Cron bleibt als No-Op bestehen, falls er irgendwo noch konfiguriert ist.
  return;
}

// App owner (admin) — only this user may perform privileged actions like world reset
const OWNER_USER_ID = "WtkwDtXZZRctZRVF"; // set from builder info

// Only this email (in the in-app / Supabase sign-in) may publish News
const OWNER_EMAIL = "coinestate117@gmail.com"; // set from builder info

// ---- Constants & Helpers ----

function _isValidDeviceId(deviceId: string) {
  // Device IDs are generated client-side and persisted per browser/device.
  // We only allow a conservative character set to avoid accidental injection.
  if (typeof deviceId !== "string") return false;
  const id = deviceId.trim();
  if (!id) return false;
  if (id.length < 8 || id.length > 128) return false;
  if (!/^dev_[a-zA-Z0-9_-]+$/.test(id)) return false;
  return true;
}

async function requireSupabaseUserId(accessToken?: string): Promise<string> {
  const token = (accessToken || "").trim();
  if (!token) {
    throw new Error("Please sign in to continue.");
  }

  try {
    const supabase = requireSupabaseClient();
    const res = await supabase.auth.getUser(token);
    const id = (res as any)?.data?.user?.id
      ? String((res as any).data.user.id)
      : "";
    if (!id) {
      throw new Error("Please sign in to continue.");
    }
    return id;
  } catch {
    throw new Error("Please sign in to continue.");
  }
}

async function requireAppUserId(input?: { accessToken?: string }) {
  // We only accept the in-app (Supabase) login.
  // This prevents any automatic redirect to the platform login screen.
  return await requireSupabaseUserId(input?.accessToken);
}

function requireGuestDeviceId(deviceId?: string): string {
  if (deviceId && _isValidDeviceId(deviceId)) return deviceId;
  throw new Error(
    "This action needs a device ID. Please reload the app (and allow cookies/storage).",
  );
}

function _isDeviceActorId(id: string) {
  return typeof id === "string" && id.startsWith("dev_");
}

async function ensureUserWithCompliance(userId: string) {
  const user = await ensureUser(userId);
  if (_isDeviceActorId(userId)) return user;

  const ok =
    !!user.termsAcceptedAt && !!user.privacyAcceptedAt && !!user.ageConfirmedAt;
  if (!ok) {
    throw new Error(
      "Please confirm Terms, Privacy Policy, and 18+ during setup first.",
    );
  }
  return user;
}

// Protected world zones (server-authoritative)
// legacy constants kept for reference (not used in island map)

// Additional protected landmarks (server-authoritative)
// legacy LAKES (unused)
// legacy GOLF_COURSES (unused)
// legacy BEACHES (unused)
// legacy MARINAS (unused)
// legacy FOOTBALL_FIELDS (unused)

// Tennis courts distributed across the world (server-authoritative)
// legacy TENNIS_COURTS (unused)

// Multiple airports distributed across the world (server-authoritative)
// legacy AIRPORTS (unused)
// New island world: 800 x 500 cells = 400,000 m² (rect bounds)
const ISLAND_W = 800;
const ISLAND_H = 500;
const ISLAND_X0 = 0;
const ISLAND_Y0 = 0;

// Irregular island mask (approx. to reference shape) defined as polygon in world coords
// Points go clockwise; tweakable without DB changes
const ISLAND_POLY: Array<{ x: number; y: number }> = (() => {
  // Normalized 0..1 polygon approximating the provided reference island silhouette (counter‑clockwise)
  const norm: Array<[number, number]> = [
    [0.02, 0.52],
    [0.04, 0.44],
    [0.08, 0.36],
    [0.14, 0.3],
    [0.22, 0.26],
    [0.32, 0.23],
    [0.45, 0.2],
    [0.55, 0.16],
    [0.62, 0.1],
    [0.67, 0.06],
    [0.72, 0.05],
    [0.78, 0.08],
    [0.82, 0.14],
    [0.84, 0.22],
    [0.88, 0.28],
    [0.94, 0.3],
    [0.97, 0.36],
    [0.96, 0.44],
    [0.92, 0.5],
    [0.88, 0.56],
    [0.86, 0.62],
    [0.88, 0.68],
    [0.92, 0.74],
    [0.94, 0.8],
    [0.92, 0.86],
    [0.86, 0.9],
    [0.78, 0.92],
    [0.7, 0.92],
    [0.62, 0.9],
    [0.54, 0.88],
    [0.46, 0.86],
    [0.38, 0.86],
    [0.3, 0.88],
    [0.22, 0.88],
    [0.16, 0.86],
    [0.1, 0.82],
    [0.06, 0.76],
    [0.04, 0.68],
    [0.03, 0.6],
  ];
  return norm.map(([nx, ny]) => ({
    x: Math.floor(ISLAND_X0 + nx * ISLAND_W),
    y: Math.floor(ISLAND_Y0 + ny * ISLAND_H),
  }));
})();

function _pointInPolygon(wx: number, wy: number, poly = ISLAND_POLY) {
  // Ray casting
  let inside = false;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const xi = poly[i]!.x,
      yi = poly[i]!.y;
    const xj = poly[j]!.x,
      yj = poly[j]!.y;
    const intersect =
      yi > wy !== yj > wy &&
      wx < ((xj - xi) * (wy - yi)) / (yj - yi + 0.0000001) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
}
// legacy ROAD_SPACING (unused)
// Areas removed: constants not used anymore

function _islandContains(wx: number, wy: number): boolean {
  // Land only if inside the irregular polygon and within rect bounds
  if (
    !(
      wx >= ISLAND_X0 &&
      wy >= ISLAND_Y0 &&
      wx < ISLAND_X0 + ISLAND_W &&
      wy < ISLAND_Y0 + ISLAND_H
    )
  )
    return false;
  return _pointInPolygon(wx, wy);
}

/* legacy seasonal protected regions (unused)
function _isSpecialProtected(wx: number, wy: number): boolean {
  const areaId = _areaIdFromWorld(wx, wy);
  if (!areaId) return false;
  const base = _areaBaseRectFromId(areaId);
  if (!base) return false;
  const rx = wx - base.x;
  const ry = wy - base.y;
  // Helper relative-rect test
  const relIn = (x: number, y: number, w: number, h: number) =>
    rx >= x && rx < x + w && ry >= y && ry < y + h;

  switch (areaId) {
    case 740: {
      // Summer luxury: lake + marina, beach, golf course
      if (relIn(10, 8, 28, 10)) return true; // lake
      if (relIn(36, 9, 6, 5)) return true; // marina on lake edge
      if (relIn(44, 20, 24, 6)) return true; // beach strip
      if (relIn(6, 26, 24, 10)) return true; // golf course
      return false;
    }
    case 1111: {
      // Winter luxury: mountain/piste, plaza with xmas tree, santa corner
      if (relIn(8, 6, 28, 22)) return true; // mountain/piste
      if (relIn(40, 8, 8, 8)) return true; // xmas tree plaza
      if (relIn(50, 18, 6, 6)) return true; // santa corner
      return false;
    }
    case 989: {
      // Autumn luxury: big park with brown leaves
      if (relIn(10, 8, 40, 20)) return true;
      return false;
    }
    case 247: {
      // Spring luxury: blossom park
      if (relIn(12, 10, 48, 20)) return true;
      return false;
    }
    default:
      return false;
  }
}
*/
// legacy rect helper (unused)

function _isProtectedPlot(wx: number, wy: number) {
  // Everything outside the irregular island is protected (non-buyable/non-buildable).
  return !_islandContains(wx, wy);
}
// Additional cities placed across the world (server-authoritative)
// legacy CITIES (unused)

// New rule: any cell inside the island is allowed
function _isAllowedLot(wx: number, wy: number) {
  if (_isProtectedPlot(wx, wy)) return false;
  return _islandContains(wx, wy);
}

// Beach band server logic removed; beach handled visually on client

// Areas removed: _areaIdFromWorld no longer used

type PricingSnapshot = {
  basePrice: number;
  stepSizeM2: number;
  stepIncreasePct: number; // legacy; kept for UI compatibility
  sellFeePct: number;
  cumulativeSoldM2: number;
  totalSupplyM2: number;
  kPerM2: number; // CHF added/removed per 1 m² net flow
  currentPrice: number; // derived from dynamic pressure
  certificateTemplateUrl: string | null;
};

type PortfolioSummary = {
  userId: string;
  balance: number;
  landOwnedM2: number;
  unassignedM2: number;
  houses: Record<HouseLevel, number>;
  estimatedValueCHF: number;
};

type HoldingsBreakdown = {
  totalM2: number;
  mainM2: number;
  secondaryM2: number;
};

type HouseLevel = "L1" | "L2" | "L3" | "L4" | "L5";

// Bonus mapping per level
const HOUSE_BONUS: Record<HouseLevel, number> = {
  L1: 0.05,
  L2: 0.12,
  L3: 0.25,
  L4: 0.45,
  L5: 0.7,
};

// Level order for freeing assignments on sell (low bonus first protects higher tiers)
const LEVELS_LOW_TO_HIGH: HouseLevel[] = ["L1", "L2", "L3", "L4", "L5"];

// Market is a singleton table (first row) – identified dynamically via DB

function toCHF(n: number) {
  return Math.round(n * 100) / 100;
}

async function _recordMarketPriceSamples(input: {
  startPrice: number;
  amountM2: number;
  kind: "BUY" | "SELL";
}) {
  const amount = Math.max(0, Math.floor(input.amountM2));
  if (!Number.isFinite(amount) || amount < 1) return;

  const maxSamples = 240;
  const target = Math.min(maxSamples, amount);
  const stepEvery = Math.max(1, Math.floor(amount / target));

  const steps: number[] = [];
  for (let i = stepEvery; i <= amount; i += stepEvery) steps.push(i);
  if (steps[steps.length - 1] !== amount) steps.push(amount);

  const baseNow = Date.now();
  const durationMs = Math.min(4000, Math.max(900, steps.length * 18));
  const deltaMs =
    steps.length <= 1 ? 0 : Math.floor(durationMs / (steps.length - 1));

  const pct = input.kind === "BUY" ? 0.004 : -0.002;

  await db.marketPriceSample.createMany({
    data: steps.map((k, idx) => {
      const price = toCHF(input.startPrice * Math.pow(1 + pct, k));
      const createdAt = new Date(baseNow - (steps.length - 1 - idx) * deltaMs);
      return { price, createdAt };
    }),
  });
}

function _islandPriceAtSoldM2(
  island: {
    startPriceUSD: number;
    stepUpPct: number;
    totalSupplyM2: number;
  },
  soldM2: number,
) {
  const start = Number(island?.startPriceUSD ?? 0);
  const stepUp = Number(island?.stepUpPct ?? 0);
  const supply = Math.max(1, Math.floor(Number(island?.totalSupplyM2 ?? 1)));
  const sold = Math.max(0, Number(soldM2 || 0));

  if (!Number.isFinite(start) || start <= 0) return 0;
  if (!Number.isFinite(stepUp) || stepUp === 0) return toCHF(start);

  // The creator chooses a max +5% “speed”, but the actual bump scales with how many m²
  // are sold in a single buy by spreading the bump across the island’s total supply.
  // Step unit = 1% of total supply.
  const stepUnitM2 = Math.max(1, supply / 100);
  const r = 1 + stepUp / 100;
  const exponent = sold / stepUnitM2;
  const price = start * Math.pow(r, exponent);
  return Number.isFinite(price) && price >= 0 ? toCHF(price) : 0;
}

async function buyersCount(): Promise<number> {
  const rows = await db.transaction.findMany({
    where: { type: "BUY" },
    select: { userId: true },
    distinct: ["userId" as any],
  } as any);
  return rows.length;
}

async function getCurrentCurvingPriceAndMaybeTrigger(): Promise<{
  price: number;
  startAt: Date | null;
  market: any;
}> {
  const market = await ensureMarket();
  const p = (market as any).currentPrice ?? market.basePrice;
  return { price: toCHF(p), startAt: null, market };
}

async function ensureMarket(): Promise<{
  id: string;
  totalSupplyM2: number;
  cumulativeSoldM2: number;
  basePrice: number;
  currentPrice: number;
  stepSizeM2: number;
  stepIncreasePct: number;
  sellFeePct: number;
  pricePressure: number;
  pressureUpdatedAt: Date;
  decayHalfLifeMinutes: number;
  kPerM2: number;
  certificateTemplateUrl: string | null;
  mapBackgroundUrl: string | null;
  curvingStartAt: Date | null;
  activePurchaseDays: number;
  lastActivePurchaseDay: Date | null;
  sellPressureM2: number;
  priceDropPct: number;
}> {
  const FULL_SUPPLY_M2 = ISLAND_W * ISLAND_H; // keep global supply constant at 400,000 m²
  let market = await db.market.findFirst();
  if (!market) {
    market = await db.market.create({
      data: {
        totalSupplyM2: FULL_SUPPLY_M2,
        cumulativeSoldM2: 0,
        basePrice: 80,
        currentPrice: 80,
        stepSizeM2: 100_000,
        stepIncreasePct: 0,
        sellFeePct: 2,
        pricePressure: 0,
        pressureUpdatedAt: new Date(),
        decayHalfLifeMinutes: 5,
        kPerM2: 1,
        activePurchaseDays: 0,
        lastActivePurchaseDay: null,
        sellPressureM2: 0,
        priceDropPct: 0,
      },
    });
  }
  // Ensure total supply remains fixed at the full grid (400,000 m²)
  if (
    market.totalSupplyM2 !== FULL_SUPPLY_M2 ||
    (market as any).currentPrice == null
  ) {
    market = await db.market.update({
      where: { id: market.id },
      data: {
        totalSupplyM2: FULL_SUPPLY_M2,
        currentPrice: (market as any).currentPrice ?? market.basePrice,
      },
    });
  }
  return market as any;
}

async function _assignUniqueStartIfMissing(user: any): Promise<any> {
  if (user.startWX != null && user.startWY != null) return user;

  // Generate a pseudo-random but deterministic point based on userId.
  // IMPORTANT: Avoid scanning the whole User table (can timeout). Instead, probe a few candidate points.
  const hash32 = (s: string) => {
    let h = 2166136261 >>> 0;
    for (let i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 16777619) >>> 0;
    }
    return h >>> 0;
  };

  const seed = hash32(user.id);

  const tryAssign = async (wx: number, wy: number): Promise<any | null> => {
    if (!_islandContains(wx, wy)) return null;

    try {
      const updated = await db.$transaction(async (tx) => {
        const taken = await tx.user.findFirst({
          where: {
            startWX: wx,
            startWY: wy,
            id: { not: user.id },
          },
          select: { id: true },
        });
        if (taken) return null;
        return await tx.user.update({
          where: { id: user.id },
          data: { startWX: wx, startWY: wy },
        });
      });

      return updated ?? null;
    } catch {
      return null;
    }
  };

  // Try a bounded number of deterministic candidates.
  // This keeps runtime predictable even with many users.
  const MAX_ATTEMPTS = 250;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    const wx = ISLAND_X0 + ((seed + attempt * 97) % ISLAND_W);
    const wy = ISLAND_Y0 + (((seed >>> 11) + attempt * 131) % ISLAND_H);

    const updated = await tryAssign(wx, wy);
    if (updated) return updated;
  }

  // Fallback: if we couldn't find a free spot, leave the user unchanged.
  return user;
}

async function ensureUser(userId: string): Promise<any> {
  const isOwner = userId === OWNER_USER_ID;
  // Use upsert to avoid race conditions when multiple requests initialize the same user.
  let user = await db.user.upsert({
    where: { id: userId },
    create: {
      id: userId,
      balance: 0,
      landOwnedM2: 0,
      isAdmin: isOwner,
    },
    update: isOwner ? { isAdmin: true } : {},
  });

  // Assign unique start if missing
  user = await _assignUniqueStartIfMissing(user);
  return user as any;
}

async function onBuyCommitted(_input: {
  buyerId: string;
  amountM2: number;
}): Promise<void> {
  void _input;
  try {
    let market = await ensureMarket();
    const today = new Date();
    const dayStart = new Date(today);
    dayStart.setHours(0, 0, 0, 0);

    if (!market.curvingStartAt) {
      const distinct = await buyersCount();
      if (distinct >= 500) {
        await db.market.update({
          where: { id: market.id },
          data: {
            curvingStartAt: new Date(),
            // Start bei 100: aktive Kauftage beginnen bei 0 und zählen erst ab dem nächsten Kauf-Tag hoch
            activePurchaseDays: 0,
            lastActivePurchaseDay: dayStart,
          },
        });
        await broadcastMarket();
      }
      return;
    }

    const last = market.lastActivePurchaseDay
      ? new Date(market.lastActivePurchaseDay)
      : null;
    if (!last || last.getTime() !== dayStart.getTime()) {
      await db.market.update({
        where: { id: market.id },
        data: {
          activePurchaseDays: (market.activePurchaseDays ?? 0) + 1,
          lastActivePurchaseDay: dayStart,
        },
      });
      await broadcastMarket();
    }
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("onBuyCommitted error", _e);
  }
}

async function applySellPressure(soldM2: number) {
  try {
    const market = await ensureMarket();
    const totalSupply = market.totalSupplyM2 || 400_000;
    const soldFrac = Math.max(0, Math.min(1, soldM2 / totalSupply));
    const currentDrop = Math.max(0, (market as any).priceDropPct ?? 0);
    // Preis fällt langsamer als er steigt: ca. 1 % runter je 3 % verkaufte Fläche
    const dropIncrease = soldFrac * (1 / 3);
    const newDrop = currentDrop + dropIncrease;
    await db.market.update({
      where: { id: market.id },
      data: { priceDropPct: newDrop },
    });
    if (dropIncrease > 0) await broadcastMarket();
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("applySellPressure error", _e);
  }
}

async function totalOwnedM2(): Promise<number> {
  const agg = await db.user.aggregate({
    _sum: { landOwnedM2: true },
  });
  return (agg._sum.landOwnedM2 as unknown as number) ?? 0;
}

let __cellToM2ScaleCache: number | null = null;
function _countLandCells(): number {
  let land = 0;
  for (let y = ISLAND_Y0; y < ISLAND_Y0 + ISLAND_H; y++) {
    for (let x = ISLAND_X0; x < ISLAND_X0 + ISLAND_W; x++) {
      if (_islandContains(x, y)) land++;
    }
  }
  return Math.max(1, land);
}
async function getCellToM2ScaleInternal(): Promise<number> {
  if (__cellToM2ScaleCache !== null) return __cellToM2ScaleCache;
  const scale = 1; // 1 Feld = 1 m²
  __cellToM2ScaleCache = scale;
  // Persist for observability
  try {
    const market = await ensureMarket();
    if ((market as any).kPerM2 !== scale) {
      await db.market.update({
        where: { id: market.id },
        data: { kPerM2: scale },
      });
    }
  } catch {}
  return scale;
}

export async function getCellToM2Scale() {
  return { scale: await getCellToM2ScaleInternal() };
}

async function getPricingSnapshot(): Promise<PricingSnapshot> {
  const { price, market } = await getCurrentCurvingPriceAndMaybeTrigger();
  return {
    basePrice: market.basePrice,
    stepSizeM2: market.stepSizeM2,
    stepIncreasePct: market.stepIncreasePct,
    sellFeePct: market.sellFeePct,
    cumulativeSoldM2: market.cumulativeSoldM2,
    totalSupplyM2: market.totalSupplyM2,
    kPerM2: 0,
    currentPrice: price,
    certificateTemplateUrl: market.certificateTemplateUrl ?? null,
  };
}

// Piecewise step pricing for large purchases crossing thresholds
// Linear slippage during a single order: price moves by k per m²
function computeDynamicBuyCost({
  amountM2,
  currentPrice,
}: {
  amountM2: number;
  currentPrice: number;
}) {
  const n = amountM2;
  const total = n * currentPrice;
  const avg = currentPrice;
  return { totalCost: toCHF(total), avgPrice: toCHF(avg) };
}

function computeDynamicSellProceeds({
  amountM2,
  currentPrice,
}: {
  amountM2: number;
  currentPrice: number;
}) {
  const n = amountM2;
  const total = n * currentPrice;
  const avg = currentPrice;
  return {
    gross: toCHF(Math.max(0, total)),
    avgPrice: toCHF(Math.max(0, avg)),
  };
}

async function getUserHouses(
  userId: string,
): Promise<Record<HouseLevel, number>> {
  const holdings = await db.houseHolding.findMany({ where: { userId } });
  const result: Record<HouseLevel, number> = {
    L1: 0,
    L2: 0,
    L3: 0,
    L4: 0,
    L5: 0,
  };
  for (const h of holdings) {
    const lvl = h.level as HouseLevel;
    if (result[lvl] !== undefined) result[lvl] = h.count;
  }
  return result;
}

function computeAssignedM2(houses: Record<HouseLevel, number>) {
  const totalHouses = houses.L1 + houses.L2 + houses.L3 + houses.L4 + houses.L5;
  return totalHouses * 100;
}

function computeEstimatedValueCHF({
  landOwnedM2,
  houses,
  currentPrice,
}: {
  landOwnedM2: number;
  houses: Record<HouseLevel, number>;
  currentPrice: number;
}) {
  const base = landOwnedM2 * currentPrice;
  const bonusM2Equivalent =
    houses.L1 * 100 * HOUSE_BONUS.L1 +
    houses.L2 * 100 * HOUSE_BONUS.L2 +
    houses.L3 * 100 * HOUSE_BONUS.L3 +
    houses.L4 * 100 * HOUSE_BONUS.L4 +
    houses.L5 * 100 * HOUSE_BONUS.L5;
  return toCHF(base + bonusM2Equivalent * currentPrice);
}

async function broadcastMarket() {
  try {
    const snapshot = await getPricingSnapshot();
    const owned = await totalOwnedM2();
    const available = snapshot.totalSupplyM2 - owned;
    await setRealtimeStore({
      channelId: "market",
      data: {
        price: snapshot.currentPrice,
        availableSupplyM2: available,
        cumulativeSoldM2: snapshot.cumulativeSoldM2,
      },
    });
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    // Non-critical
    console.error("broadcastMarket error", _e);
  }
}

// ---- Public API Endpoints ----

// ===== Secondary Market (no schema changes; uses Transaction meta) =====

function _txMetaParse<T>(s?: string | null): T | null {
  if (!s) return null;
  try {
    return JSON.parse(s) as T;
  } catch {
    return null;
  }
}

type ListingMeta = {
  x: number;
  y: number;
  priceEUR: number;
  sellerId: string;
  islandId?: string;
};

async function _getActiveListingsMap() {
  // Gather list, cancel and sold events in order
  const rows = await db.transaction.findMany({
    where: {
      type: { in: ["LIST", "LIST_CANCEL", "SECONDARY_SOLD"] },
    },
    orderBy: { createdAt: "asc" },
  });
  const map = new Map<
    string,
    {
      x: number;
      y: number;
      priceEUR: number;
      sellerId: string;
      listedAt: Date;
      islandId?: string;
    }
  >();
  for (const r of rows) {
    const meta = _txMetaParse<ListingMeta>(r.meta);
    const key = meta ? `${meta.x}:${meta.y}` : "";
    if (!meta || !key) continue;
    if (r.type === "LIST") {
      map.set(key, {
        x: meta.x,
        y: meta.y,
        priceEUR: meta.priceEUR,
        sellerId: meta.sellerId,
        listedAt: r.createdAt,
        islandId: meta.islandId,
      });
    } else if (r.type === "LIST_CANCEL" || r.type === "SECONDARY_SOLD") {
      map.delete(key);
    }
  }
  // Validate ownership is still with seller; remove stale
  for (const [key, l] of Array.from(map.entries())) {
    if ((l as any).islandId) {
      const holding = await db.islandHolding.findFirst({
        where: { islandId: String((l as any).islandId), x: l.x, y: l.y },
      });
      if (!holding || holding.ownerId !== l.sellerId) map.delete(key);
    } else {
      const p = await db.plot.findFirst({ where: { x: l.x, y: l.y } });
      if (!p || p.ownerId !== l.sellerId) map.delete(key);
    }
  }
  return map;
}

export async function listSecondaryListings() {
  try {
    const map = await _getActiveListingsMap();
    const items = await Promise.all(
      Array.from(map.values()).map(async (l) => {
        try {
          const plot = await db.plot.findFirst({ where: { x: l.x, y: l.y } });
          const seller = await db.user.findUnique({
            where: { id: l.sellerId },
            select: {
              id: true,
              displayNameLocal: true,
              name: true,
              handle: true,
              profileImageUrl: true,
              image: true,
            },
          });
          const sellerName = seller
            ? seller.displayNameLocal ||
              seller.name ||
              (seller.handle
                ? "@" + seller.handle
                : `User ${seller.id.slice(0, 6)}`)
            : `User ${l.sellerId.slice(0, 6)}`;
          const sellerAvatarUrl =
            seller?.profileImageUrl || seller?.image || null;

          if (!plot)
            return {
              ...l,
              certificateId: null as string | null,
              purchasedAt: null as Date | null,
              sellerName,
              sellerAvatarUrl,
              registered: !!seller,
            };
          const cert = await db.certificate.findFirst({
            where: { plotId: plot.id, userId: l.sellerId },
            orderBy: { purchasedAt: "asc" },
            select: { id: true, purchasedAt: true },
          });
          return {
            ...l,
            certificateId: cert?.id ?? null,
            purchasedAt: cert?.purchasedAt ?? null,
            sellerName,
            sellerAvatarUrl,
            registered: !!seller,
          };
        } catch {
          return {
            ...l,
            certificateId: null as string | null,
            purchasedAt: null as Date | null,
            sellerName: `User ${l.sellerId.slice(0, 6)}`,
            sellerAvatarUrl: null as string | null,
            registered: false,
          };
        }
      }),
    );
    return items.sort((a, b) => a.priceEUR - b.priceEUR);
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("listSecondaryListings error", _e);
    throw _e;
  }
}

const SECONDARY_HOLD_MS = 5 * 24 * 60 * 60 * 1000;

async function _requireSecondaryHoldPeriodPassed(input: {
  userId: string;
  x: number;
  y: number;
  islandId?: string | null;
}) {
  // Islands have no holding time: you can buy & sell/list again immediately.
  if (input.islandId) return;

  const cutoff = new Date(Date.now() - SECONDARY_HOLD_MS);
  const txs = await db.transaction.findMany({
    where: {
      userId: input.userId,
      type: "SECONDARY_BUY",
      createdAt: { gte: cutoff },
    },
    select: { meta: true, createdAt: true },
    orderBy: { createdAt: "desc" },
    take: 250,
  });

  const targetIslandId = input.islandId ? String(input.islandId) : null;

  for (const tx of txs) {
    const meta = _txMetaParse<any>(tx.meta);
    const mx = Math.floor(Number(meta?.x ?? NaN));
    const my = Math.floor(Number(meta?.y ?? NaN));
    const mIslandId = meta?.islandId ? String(meta.islandId) : null;

    // Hold period is scoped to the same market context (map vs specific island).
    if (
      mx === input.x &&
      my === input.y &&
      (mIslandId ?? null) === (targetIslandId ?? null)
    ) {
      const unlockAt = new Date(
        new Date(tx.createdAt as any).getTime() + SECONDARY_HOLD_MS,
      );
      const msLeft = unlockAt.getTime() - Date.now();
      const daysLeft = Math.max(0, Math.ceil(msLeft / 86400000));
      throw new Error(
        `This 2nd Market m² has a 5-day holding period. You can list it in ${daysLeft} day(s).`,
      );
    }
  }
}

export async function createSecondaryListing(input: {
  x: number;
  y: number;
  priceEUR: number;
  islandId?: string;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const me = await ensureUser(userId);
    const x = Math.floor(input.x),
      y = Math.floor(input.y);
    if (!Number.isFinite(x) || !Number.isFinite(y))
      throw new Error("Invalid coordinates.");
    const price = Math.round((input.priceEUR ?? 0) * 100) / 100;
    if (!Number.isFinite(price) || price <= 0)
      throw new Error("Invalid price.");
    // Ownership check: global map uses Plot; islands use IslandHolding
    let ownedPlotId: string | null = null;

    if (input.islandId) {
      const holding = await db.islandHolding.findFirst({
        where: { islandId: input.islandId, x, y },
      });
      if (!holding || holding.ownerId !== me.id)
        throw new Error("You do not own this plot.");
    } else {
      const plot = await db.plot.findFirst({ where: { x, y } });
      if (!plot || plot.ownerId !== me.id)
        throw new Error("You do not own this plot.");
      ownedPlotId = plot.id;
    }

    // If this m² was bought on the 2nd Market, the buyer must hold it for 5 days
    // before they can list it again on the 2nd Market.
    await _requireSecondaryHoldPeriodPassed({
      userId: me.id,
      x,
      y,
      islandId: input.islandId ?? null,
    });

    const active = await _getActiveListingsMap();
    if (active.has(`${x}:${y}`))
      throw new Error("This plot is already listed.");

    // Ensure the plot has a certificate tied to it (so 2nd Market listings can always show one).
    // This only applies to the global map (main-market) m².
    if (!input.islandId && ownedPlotId) {
      const certAny = await db.certificate.findFirst({
        where: { plotId: ownedPlotId },
        orderBy: { purchasedAt: "asc" },
        select: { id: true, userId: true },
      });

      if (certAny) {
        if (certAny.userId !== me.id) {
          await db.certificate.update({
            where: { id: certAny.id },
            data: { userId: me.id },
          });
        }
      } else {
        const unassigned = await db.certificate.findFirst({
          where: { userId: me.id, plotId: null },
          orderBy: { purchasedAt: "asc" },
          select: { id: true },
        });

        if (unassigned) {
          await db.certificate.update({
            where: { id: unassigned.id },
            data: { plotId: ownedPlotId },
          });
        } else {
          await db.certificate.create({
            data: {
              userId: me.id,
              plotId: ownedPlotId,
              purchasedAt: new Date(),
            },
          });
        }
      }
    }
    await db.transaction.create({
      data: {
        userId: me.id,
        type: "LIST",
        m2: 1,
        totalAmountCHF: 0,
        meta: JSON.stringify({
          x,
          y,
          priceEUR: price,
          sellerId: me.id,
          islandId: input.islandId,
        } satisfies ListingMeta),
      },
    });
    await setRealtimeStore({
      channelId: "secondary",
      data: { ts: Date.now(), event: "listed", x, y, priceEUR: price },
    });
    return { ok: true as const };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("createSecondaryListing error", _e);
    throw _e;
  }
}

export async function quickSellOwnedPlots(input: {
  pricesEUR: number[];
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const me = await ensureUser(userId);

    const raw = Array.isArray(input.pricesEUR) ? input.pricesEUR : [];
    const requested = raw.length;
    if (!requested) throw new Error("Invalid m² amount.");

    const prices = raw.map((p) => Math.round((p ?? 0) * 100) / 100);
    if (prices.some((p) => !Number.isFinite(p) || p <= 0))
      throw new Error("Invalid price.");

    // Finde alle eigenen Plots
    const plots = await db.plot.findMany({
      where: { ownerId: me.id },
      select: { x: true, y: true },
      orderBy: { createdAt: "asc" },
    });

    if (!plots.length)
      throw new Error("You currently have no m² that can be listed.");

    // Filtere bereits gelistete Felder heraus
    const active = await _getActiveListingsMap();

    // Also enforce the 5-day holding rule for plots bought on the 2nd Market.
    const cutoff = new Date(Date.now() - SECONDARY_HOLD_MS);
    const recentSecondaryBuys = await db.transaction.findMany({
      where: {
        userId: me.id,
        type: "SECONDARY_BUY",
        createdAt: { gte: cutoff },
      },
      select: { meta: true },
      orderBy: { createdAt: "desc" },
      take: 500,
    });
    const locked = new Set<string>();
    for (const tx of recentSecondaryBuys) {
      const meta = _txMetaParse<any>(tx.meta);
      const mx = Math.floor(Number(meta?.x ?? NaN));
      const my = Math.floor(Number(meta?.y ?? NaN));
      if (Number.isFinite(mx) && Number.isFinite(my)) {
        locked.add(`${mx}:${my}`);
      }
    }

    const freePlots = plots.filter(
      (p) => !active.has(`${p.x}:${p.y}`) && !locked.has(`${p.x}:${p.y}`),
    );

    if (!freePlots.length)
      throw new Error(
        "There are no free m² available for listing (some may still be in the 5-day holding period).",
      );

    const count = Math.min(requested, freePlots.length);
    const toCreate = freePlots.slice(0, count);

    for (let i = 0; i < toCreate.length; i++) {
      const p = toCreate[i]!;
      const price = prices[i]!;

      await db.transaction.create({
        data: {
          userId: me.id,
          type: "LIST",
          m2: 1,
          totalAmountCHF: 0,
          meta: JSON.stringify({
            x: p.x,
            y: p.y,
            priceEUR: price,
            sellerId: me.id,
          } satisfies ListingMeta),
        },
      });
      await setRealtimeStore({
        channelId: "secondary",
        data: {
          ts: Date.now(),
          event: "listed",
          x: p.x,
          y: p.y,
          priceEUR: price,
        },
      });
    }

    return { created: toCreate.length };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("quickSellOwnedPlots error", _e);
    throw _e;
  }
}

export async function quickSellIslandHoldingsByAmount(input: {
  islandId: string;
  amountM2: number;
  priceEUR: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const me = await ensureUser(userId);

    const islandId = String(input.islandId || "").trim();
    if (!islandId) throw new Error("Island is required.");

    const amountM2 = Math.max(0, Math.floor(Number(input.amountM2 || 0)));
    if (!amountM2) throw new Error("Invalid m² amount.");

    // Safety cap to avoid accidental huge listing batches from a typo.
    if (amountM2 > 500) {
      throw new Error("Too many m² at once. Please sell 500 or less.");
    }

    const priceEUR = Math.round((Number(input.priceEUR || 0) || 0) * 100) / 100;
    if (!Number.isFinite(priceEUR) || priceEUR <= 0)
      throw new Error("Invalid price.");

    // Find all island holdings owned by the user
    const holdings = await db.islandHolding.findMany({
      where: {
        islandId,
        ownerId: me.id,
      },
      select: {
        x: true,
        y: true,
      },
    });

    if (!holdings.length) {
      throw new Error("You don't own any m² on this island.");
    }

    // Filter out already listed m²
    const active = await _getActiveListingsMap();
    const freeHoldings = holdings
      .map((h) => ({ x: Math.floor(h.x), y: Math.floor(h.y) }))
      .filter((h) => !active.has(`${h.x}:${h.y}`));

    if (!freeHoldings.length) {
      throw new Error(
        "There are no free m² available for listing on this island right now.",
      );
    }

    freeHoldings.sort((a, b) => a.y - b.y || a.x - b.x);

    const toCreate = freeHoldings.slice(
      0,
      Math.min(amountM2, freeHoldings.length),
    );

    // Create listings (one transaction per m²)
    await db.transaction.createMany({
      data: toCreate.map((p) => ({
        userId: me.id,
        type: "LIST",
        m2: 1,
        totalAmountCHF: 0,
        meta: JSON.stringify({
          x: p.x,
          y: p.y,
          priceEUR,
          sellerId: me.id,
          islandId,
        } satisfies ListingMeta),
      })),
    });

    // Single realtime tick so clients refresh listings
    await setRealtimeStore({
      channelId: "secondary",
      data: {
        ts: Date.now(),
        event: "listed_bulk",
        islandId,
        created: toCreate.length,
      },
    });

    return {
      created: toCreate.length,
      requested: amountM2,
      available: freeHoldings.length,
    };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("quickSellIslandHoldingsByAmount error", _e);
    throw _e;
  }
}

export async function cancelSecondaryListing(input: {
  x: number;
  y: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const me = await ensureUser(userId);
    const x = Math.floor(input.x),
      y = Math.floor(input.y);
    const active = await _getActiveListingsMap();
    const l = active.get(`${x}:${y}`);
    if (!l) throw new Error("No active listing for these coordinates.");
    if (l.sellerId !== me.id)
      throw new Error("You can only cancel your own listings.");
    await db.transaction.create({
      data: {
        userId: me.id,
        type: "LIST_CANCEL",
        m2: 1,
        totalAmountCHF: 0,
        meta: JSON.stringify({ x, y }),
      },
    });
    await setRealtimeStore({
      channelId: "secondary",
      data: { ts: Date.now(), event: "cancel", x, y },
    });
    return { ok: true as const };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("cancelSecondaryListing error", _e);
    throw _e;
  }
}

async function _buySecondaryListingWithKnownListing(input: {
  buyerId: string;
  x: number;
  y: number;
  priceEUR: number;
  sellerId: string;
  islandIdFromListing: string | null;
}): Promise<{
  ok: true;
  x: number;
  y: number;
  priceEUR: number;
  islandId: string | null;
  purchasedAt: Date;
  affectedUserIds: string[];
  mapHouseType: string;
  certificate: {
    id: string;
    purchasedAt: Date;
    plot: { x: number; y: number };
  } | null;
}> {
  const x = Math.floor(input.x);
  const y = Math.floor(input.y);
  const price = Number(input.priceEUR ?? 0) || 0;
  if (!Number.isFinite(price) || price <= 0) {
    throw new Error("Invalid listing price.");
  }

  const buyer = await ensureUser(input.buyerId);
  if (input.sellerId === buyer.id) {
    throw new Error("You cannot buy your own listing.");
  }

  const islandIdFromListing = input.islandIdFromListing
    ? String(input.islandIdFromListing)
    : null;

  const plot = islandIdFromListing
    ? null
    : await db.plot.findFirst({ where: { x, y } });

  if (islandIdFromListing) {
    const holding = await db.islandHolding.findFirst({
      where: { islandId: islandIdFromListing, x, y },
    });
    if (!holding || holding.ownerId !== input.sellerId) {
      throw new Error("This listing is no longer valid.");
    }
  } else {
    if (!plot || plot.ownerId !== input.sellerId) {
      throw new Error("This listing is no longer valid.");
    }
  }

  if ((buyer.balance ?? 0) < price) throw new Error("Insufficient balance");

  const seller = await db.user.findUnique({ where: { id: input.sellerId } });
  if (!seller) throw new Error("Seller not found.");

  const affectedUserIds = new Set<string>([buyer.id, seller.id]);

  // Atomic buyer update prevents double-spend
  const buyerAffected = await db.$executeRaw`
    UPDATE User
    SET
      balance = ROUND(COALESCE(balance, 0) - ${price}, 2),
      landOwnedM2 = COALESCE(landOwnedM2, 0) + 1,
      secondaryMarketPurchasedM2 = COALESCE(secondaryMarketPurchasedM2, 0) + 1
    WHERE id = ${buyer.id}
      AND COALESCE(balance, 0) >= ${price}
  `;
  if (!buyerAffected) throw new Error("Insufficient balance");

  await db.$executeRaw`
    UPDATE User
    SET
      balance = ROUND(COALESCE(balance, 0) + ${price}, 2),
      landOwnedM2 = CASE
        WHEN COALESCE(landOwnedM2, 0) > 0 THEN COALESCE(landOwnedM2, 0) - 1
        ELSE 0
      END
    WHERE id = ${seller.id}
  `;

  // Transfer ownership: map uses Plot; islands use IslandHolding
  let finalCert: any = null;
  let mapHouseType = "NONE";

  if (islandIdFromListing) {
    const holding = await db.islandHolding.findFirst({
      where: { islandId: islandIdFromListing, x, y },
    });
    if (!holding || holding.ownerId !== input.sellerId) {
      throw new Error("This listing is no longer valid.");
    }
    await db.islandHolding.update({
      where: { id: holding.id },
      data: { ownerId: buyer.id },
    });
    mapHouseType = "NONE";
  } else {
    if (!plot) throw new Error("This listing is no longer valid.");
    const updatedPlot = await db.plot.update({
      where: { id: plot.id },
      data: { ownerId: buyer.id },
    });
    mapHouseType = plot?.houseType ?? "NONE";

    const cert = await db.certificate.findFirst({
      where: { plotId: updatedPlot.id, userId: seller.id },
    });
    if (cert) {
      finalCert = await db.certificate.update({
        where: { id: cert.id },
        data: { userId: buyer.id, purchasedAt: new Date() },
      });
    } else {
      finalCert = await db.certificate.create({
        data: {
          userId: buyer.id,
          plotId: updatedPlot.id,
          purchasedAt: new Date(),
        },
      });
    }
  }

  // Profit + commission estimate
  let baseUSD = price;
  let profitGrossUSD = 0;
  let commissionRate = 0.06;
  let commissionOwnerId: string | null = OWNER_USER_ID;
  try {
    const lastBuy = await db.transaction.findFirst({
      where: { userId: seller.id, type: "BUY" },
      orderBy: { createdAt: "desc" },
      select: { avgPricePerM2: true },
    });
    baseUSD =
      typeof lastBuy?.avgPricePerM2 === "number" && lastBuy.avgPricePerM2 > 0
        ? toCHF(lastBuy.avgPricePerM2)
        : price;
    profitGrossUSD = Math.max(0, toCHF(price - baseUSD));

    if (islandIdFromListing) {
      const island = await db.island.findUnique({
        where: { id: islandIdFromListing },
        select: { ownerId: true, commissionPct: true },
      });
      if (island?.ownerId) {
        commissionOwnerId = island.ownerId;
        const pct = Number((island as any).commissionPct ?? 5);
        if (Number.isFinite(pct) && pct >= 0) commissionRate = pct / 100;
      }
    }
  } catch {
    // keep defaults
  }

  const commissionUSD = toCHF(profitGrossUSD * commissionRate);
  const netSaleUSD = toCHF(price - (commissionUSD > 0 ? commissionUSD : 0));
  const netProfitUSD = toCHF(
    Math.max(0, profitGrossUSD - (commissionUSD > 0 ? commissionUSD : 0)),
  );

  await db.transaction.create({
    data: {
      userId: buyer.id,
      type: "SECONDARY_BUY",
      m2: 1,
      totalAmountCHF: price,
      meta: JSON.stringify({
        x,
        y,
        sellerId: seller.id,
        priceUSD: price,
        islandId: islandIdFromListing,
      }),
    },
  });
  await db.transaction.create({
    data: {
      userId: seller.id,
      type: "SECONDARY_SOLD",
      m2: 1,
      totalAmountCHF: price,
      meta: JSON.stringify({
        x,
        y,
        buyerId: buyer.id,
        priceUSD: price,
        baseUSD,
        profitGrossUSD,
        commissionUSD,
        netSaleUSD,
        netProfitUSD,
        commissionRate,
        islandId: islandIdFromListing,
      }),
    },
  });

  try {
    if (commissionUSD > 0.01 && commissionOwnerId) {
      const sellerNow = await db.user.findUnique({
        where: { id: seller.id },
        select: { balance: true },
      });
      const currentSellerBal = toCHF(sellerNow?.balance ?? 0);
      const deduct = Math.min(commissionUSD, currentSellerBal);
      if (deduct > 0.0) {
        await db.user.update({
          where: { id: seller.id },
          data: { balance: toCHF(currentSellerBal - deduct) },
        });
        const owner = await ensureUser(commissionOwnerId);
        const ownerBal = toCHF(owner.balance ?? 0);
        await db.user.update({
          where: { id: owner.id },
          data: { balance: toCHF(ownerBal + deduct) },
        });

        await db.transaction.create({
          data: {
            userId: seller.id,
            type: "COMMISSION_OUT",
            m2: 0,
            avgPricePerM2: 0,
            feePct: 0,
            totalAmountCHF: deduct,
            meta: JSON.stringify({
              on: "SECONDARY_SOLD",
              x,
              y,
              saleUSD: price,
              baseUSD,
              profitUSD: profitGrossUSD,
              rate: commissionRate,
              islandId: islandIdFromListing,
            }),
          },
        });
        await db.transaction.create({
          data: {
            userId: owner.id,
            type: "COMMISSION_IN",
            m2: 0,
            avgPricePerM2: 0,
            feePct: 0,
            totalAmountCHF: deduct,
            meta: JSON.stringify({
              fromUserId: seller.id,
              on: "SECONDARY_SOLD",
              x,
              y,
              islandId: islandIdFromListing,
            }),
          },
        });

        affectedUserIds.add(owner.id);
      }
    }
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("commission calculation (secondary sold) failed", _e);
  }

  try {
    const profile = await db.payoutProfile.findUnique({
      where: { userId: seller.id },
    });
    if (profile?.autoPayoutOnProfit && profile.acceptedPayoutTosAt) {
      const sellerAfter = await db.user.findUnique({
        where: { id: seller.id },
        select: { balance: true },
      });
      const currentBalance = toCHF(sellerAfter?.balance ?? 0);
      const payoutAmount = Math.min(price, currentBalance);
      if (payoutAmount > 0.01) {
        const methodSnapshot = JSON.stringify({
          method: profile.method,
          paypalEmail: profile.paypalEmail,
          iban: profile.iban,
          accountHolderName: profile.accountHolderName,
          country: profile.country,
        });
        await db.user.update({
          where: { id: seller.id },
          data: { balance: toCHF(currentBalance - payoutAmount) },
        });
        await db.withdrawalRequest.create({
          data: {
            userId: seller.id,
            amountCHF: toCHF(payoutAmount),
            status: "PENDING",
            methodSnapshot,
            note: "Auto‑Payout (2nd Market)",
          },
        });
      }
    }
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("auto payout (secondary sold) failed", _e);
  }

  const purchasedAt = (finalCert?.purchasedAt ?? new Date()) as any;

  return {
    ok: true as const,
    x,
    y,
    priceEUR: price,
    islandId: islandIdFromListing,
    purchasedAt: new Date(purchasedAt as any),
    affectedUserIds: Array.from(affectedUserIds),
    mapHouseType,
    certificate: finalCert
      ? {
          id: finalCert.id,
          purchasedAt: new Date(purchasedAt as any),
          plot: { x, y },
        }
      : null,
  };
}

export async function buySecondaryListing(input: {
  x: number;
  y: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const x = Math.floor(input.x),
      y = Math.floor(input.y);

    const active = await _getActiveListingsMap();
    const l = active.get(`${x}:${y}`);
    if (!l) throw new Error("This plot is not listed.");

    const islandIdFromListing = (l as any)?.islandId
      ? String((l as any).islandId)
      : null;

    const result = await _buySecondaryListingWithKnownListing({
      buyerId: userId,
      x,
      y,
      priceEUR: l.priceEUR,
      sellerId: l.sellerId,
      islandIdFromListing,
    });

    await setRealtimeStore({
      channelId: "wallet",
      data: {
        ts: Date.now(),
        affectedUserIds: result.affectedUserIds,
      },
    });

    await setRealtimeStore({
      channelId: "map",
      data: {
        ts: Date.now(),
        changes: [
          {
            x: result.x,
            y: result.y,
            ownerId: userId,
            houseType:
              result.islandId && result.islandId.length > 0
                ? "NONE"
                : result.mapHouseType,
          },
        ],
      },
    });

    await setRealtimeStore({
      channelId: "secondary",
      data: { ts: Date.now(), event: "sold", x: result.x, y: result.y },
    });

    // Keep old response shape
    return {
      ok: true as const,
      certificate: result.certificate,
      plot: { x: result.x, y: result.y },
      purchasedAt: result.purchasedAt as any,
      priceEUR: result.priceEUR,
      islandId: result.islandId,
    };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("buySecondaryListing error", _e);
    throw _e;
  }
}

export async function buySecondaryListingsBulk(input: {
  islandId: string;
  quantity: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const islandId = String(input.islandId || "");
    if (!islandId) throw new Error("Island is required.");

    const requested = Math.max(0, Math.floor(Number(input.quantity ?? 0)));
    if (requested <= 0) throw new Error("Quantity must be at least 1.");

    const MAX = 200;
    const target = Math.min(MAX, requested);

    const progressChannelId = `bulkbuy:${userId}:${islandId}`;
    try {
      await setRealtimeStore({
        channelId: progressChannelId,
        data: { ts: Date.now(), bought: 0, target },
      });
    } catch {}

    const active = await _getActiveListingsMap();
    const allCandidates = Array.from(active.values())
      .filter((l: any) => String((l as any).islandId ?? "") === islandId)
      .filter((l: any) => String(l.sellerId) !== String(userId))
      .sort((a: any, b: any) => (a?.priceEUR ?? 0) - (b?.priceEUR ?? 0));

    if (allCandidates.length === 0) {
      return {
        requested: target,
        bought: 0,
        totalSpent: 0,
        islandId,
      };
    }

    let bought = 0;
    let totalSpent = 0;
    const affectedUserIds = new Set<string>();
    const mapChanges: Array<{
      x: number;
      y: number;
      ownerId: string;
      houseType: string;
    }> = [];

    for (const l of allCandidates) {
      if (bought >= target) break;
      try {
        const res = await _buySecondaryListingWithKnownListing({
          buyerId: userId,
          x: l.x,
          y: l.y,
          priceEUR: l.priceEUR,
          sellerId: l.sellerId,
          islandIdFromListing: (l as any)?.islandId
            ? String((l as any).islandId)
            : null,
        });
        bought++;
        totalSpent = Math.round((totalSpent + (res.priceEUR ?? 0)) * 100) / 100;
        (res.affectedUserIds ?? []).forEach((id) => affectedUserIds.add(id));
        mapChanges.push({
          x: res.x,
          y: res.y,
          ownerId: userId,
          houseType: "NONE",
        });

        if (bought === target || bought % 5 === 0) {
          try {
            void setRealtimeStore({
              channelId: progressChannelId,
              data: { ts: Date.now(), bought, target },
            }).catch(() => null);
          } catch {}
        }
      } catch (err: any) {
        const msg = String(err?.message ?? "").toLowerCase();
        const isGone =
          msg.includes("not listed") ||
          msg.includes("no longer valid") ||
          msg.includes("already been") ||
          msg.includes("already sold");
        const isBalance = msg.includes("insufficient balance");

        if (isBalance) throw err;
        if (isGone) continue;
        throw err;
      }
    }

    try {
      void setRealtimeStore({
        channelId: progressChannelId,
        data: { ts: Date.now(), bought, target },
      }).catch(() => null);
    } catch {}

    // Single realtime updates for smoother/faster UX
    if (affectedUserIds.size > 0) {
      await setRealtimeStore({
        channelId: "wallet",
        data: {
          ts: Date.now(),
          affectedUserIds: Array.from(affectedUserIds),
        },
      });
    }
    if (mapChanges.length > 0) {
      await setRealtimeStore({
        channelId: "map",
        data: {
          ts: Date.now(),
          changes: mapChanges,
        },
      });
    }
    await setRealtimeStore({
      channelId: "secondary",
      data: { ts: Date.now(), event: "sold_bulk", islandId, bought },
    });

    return {
      requested: target,
      bought,
      totalSpent: Math.round(totalSpent * 100) / 100,
      islandId,
    };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("buySecondaryListingsBulk error", _e);
    throw _e;
  }
}

// Monetization: Echtgeld/Stripe ist deaktiviert (keine Produkte mehr).

export async function _seedCreateTopUpProducts() {
  // Echtgeld/Stripe ist deaktiviert – keine Produkte mehr nötig.
  return { ok: true as const, created: 0 };
}

export async function _seedAppConfig() {
  await db.appConfig.upsert({
    where: { key: "DEFAULT" },
    create: { key: "DEFAULT", reportInboxEmail: "" },
    update: {},
  });
}

export async function listTopUpProducts() {
  // Echtgeld/Stripe ist deaktiviert. Es gibt keine Kauf-Links mehr.
  return [] as Array<{
    id: string;
    name: string;
    priceUSD: number;
    purchaseLink: string;
  }>;
}

// Return or create a real-money purchase link for an exact EUR amount
export async function getTopUpPurchaseLink(input: { amountUSD: number }) {
  // Echtgeld/Stripe ist deaktiviert.
  void input;
  throw new Error(
    "Real-money top-ups are disabled. Please use 'Add PP coins'.",
  );
}

export async function syncTopUpCredits() {
  // Echtgeld/Stripe ist deaktiviert. Es gibt keine externen Käufe zum Synchronisieren.
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const user = await ensureUser(auth.userId);
  const updated = await db.user.findUnique({
    where: { id: user.id },
    select: { balance: true },
  });
  return {
    creditedUSD: 0,
    balanceUSD: toCHF(updated?.balance ?? 0),
  };
}

// Admin-only: instantly credit demo funds to the current user's balance
export async function creditDemoFunds(input?: { amountUSD?: number }) {
  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    if (!user.isAdmin) {
      throw new Error("Admins only.");
    }

    const amount = Math.round(((input?.amountUSD ?? 1000) + 0) * 100) / 100;
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error("Invalid amount.");
    }

    const newBalance = toCHF((user.balance ?? 0) + amount);

    await db.user.update({
      where: { id: user.id },
      data: { balance: newBalance },
    });

    // Record as a TOPUP with demo flag for transparency in history
    await db.transaction.create({
      data: {
        userId: user.id,
        type: "TOPUP",
        m2: 0,
        avgPricePerM2: 0,
        feePct: 0,
        totalAmountCHF: amount,
        meta: JSON.stringify({ demo: true, note: "Admin demo credit" }),
      },
    });

    return { ok: true as const, balanceUSD: newBalance };
  } catch (error) {
    console.error("creditDemoFunds error", error);
    throw error;
  }
}

// Demo top-up for any user: add arbitrary amount to wallet (for testing)
export async function addDemoFunds(input: {
  deviceId?: string;
  accessToken?: string;
  amountUSD: number;
}) {
  // Backwards-compatible alias (older UI flows may still call this).
  return await addPPCoins({
    deviceId: input.deviceId,
    accessToken: input.accessToken,
    amountUSD: input.amountUSD,
  });
}

export async function addPPCoins(input: {
  deviceId?: string;
  accessToken?: string;
  amountUSD: number;
}) {
  try {
    // Top-ups are only allowed for signed-in users.
    // If a Supabase accessToken is provided, use it. Otherwise require platform login.
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    const raw = Number(input?.amountUSD ?? 0);
    const amount = Math.round(raw * 100) / 100;
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error("Invalid amount.");
    }

    // Safety limit against accidental huge inputs
    const MAX = 1_000_000;
    if (amount > MAX) {
      throw new Error(`Maximum ${MAX} per top-up.`);
    }

    const newBalance = toCHF((user.balance ?? 0) + amount);

    await db.$transaction(async (tx) => {
      await tx.user.update({
        where: { id: user.id },
        data: { balance: newBalance },
      });

      await tx.transaction.create({
        data: {
          userId: user.id,
          type: "TOPUP",
          m2: 0,
          avgPricePerM2: 0,
          feePct: 0,
          totalAmountCHF: amount,
          idempotencyKey: `MANUAL_TOPUP:${user.id}:${Date.now()}:${Math.random()}`,
          meta: JSON.stringify({ source: "MANUAL", note: "PP coin top-up" }),
        },
      });
    });

    return { ok: true as const, balanceUSD: newBalance };
  } catch (error) {
    console.error("addPPCoins error", error);
    throw error;
  }
}

// Onboarding status for current user
// Admin helpers for product management
export async function adminListAllProducts() {
  // Echtgeld/Stripe ist deaktiviert – es gibt keine Produkte mehr zu verwalten.
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const me = await ensureUser(auth.userId);
  if (!me.isAdmin) throw new Error("Admins only.");
  return [] as any[];
}

export async function adminDiscontinueProduct(input: { productId: string }) {
  // Echtgeld/Stripe ist deaktiviert – nichts mehr zu deaktivieren.
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const me = await ensureUser(auth.userId);
  if (!me.isAdmin) throw new Error("Admins only.");
  void input;
  return { ok: true as const };
}

export async function getOnboarding(input?: { accessToken?: string | null }) {
  try {
    const auth = await getAuth({ required: false });

    // IMPORTANT: If we have a Supabase access token, always use Supabase identity.
    // Otherwise we can end up completing onboarding for the Adaptive userId,
    // while gameplay actions use the Supabase userId.
    const userId = input?.accessToken
      ? await requireSupabaseUserId(input.accessToken)
      : auth.status === "authenticated" && auth.userId
        ? auth.userId
        : null;

    if (!userId) {
      return {
        needsOnboarding: false,
        displayName: "",
        profileImageUrl: null,
        tosAccepted: false,
        privacyAccepted: false,
        ageConfirmed: false,
      };
    }

    const user = await ensureUser(userId);

    const hasAllOnboardingFields =
      !!user.displayNameLocal &&
      !!user.termsAcceptedAt &&
      !!user.privacyAcceptedAt &&
      !!user.ageConfirmedAt;

    // Backfill: If user already completed everything in the past, mark onboarding as completed
    // so the form won't show again.
    let onboardingCompletedAt = user.onboardingCompletedAt;
    if (!onboardingCompletedAt && hasAllOnboardingFields) {
      const now = new Date();
      await db.user.update({
        where: { id: user.id },
        data: { onboardingCompletedAt: now },
        select: { id: true },
      });
      onboardingCompletedAt = now;
    }

    const needsOnboarding = !onboardingCompletedAt;

    return {
      needsOnboarding,
      displayName: user.displayNameLocal ?? "",
      profileImageUrl: user.profileImageUrl ?? null,
      tosAccepted: !!user.termsAcceptedAt,
      privacyAccepted: !!user.privacyAcceptedAt,
      ageConfirmed: !!user.ageConfirmedAt,
    };
  } catch (error) {
    console.error("getOnboarding error", error);
    // Return safe default rather than throwing to avoid noisy unauth flows
    return {
      needsOnboarding: false,
      displayName: "",
      profileImageUrl: null,
      tosAccepted: false,
      privacyAccepted: false,
      ageConfirmed: false,
    };
  }
}

export async function completeOnboarding(input: {
  accessToken?: string | null;
  displayName: string;
  profileImageBase64?: string;
  acceptTos?: boolean;
  acceptPrivacy?: boolean;
  confirmAge18?: boolean;
}) {
  try {
    const auth = await getAuth({ required: false });

    // IMPORTANT: If we have a Supabase access token, always use Supabase identity.
    const userId = input?.accessToken
      ? await requireSupabaseUserId(input.accessToken)
      : auth.status === "authenticated" && auth.userId
        ? auth.userId
        : null;

    if (!userId) {
      throw new Error("Please sign in to continue.");
    }

    const user = await ensureUser(userId);

    const desiredName = (input.displayName ?? "").trim();
    if (!desiredName) {
      throw new Error("Please enter a username.");
    }

    const acceptTos = !!input.acceptTos;
    const acceptPrivacy = !!input.acceptPrivacy;
    const confirmAge18 = !!input.confirmAge18;

    if (!acceptTos) {
      throw new Error("Please accept the Terms & Conditions.");
    }
    if (!acceptPrivacy) {
      throw new Error("Please accept the Privacy Policy.");
    }
    if (!confirmAge18) {
      throw new Error("Please confirm that you are at least 18 years old.");
    }

    // Enforce unique username (case-insensitive) at first-time setup
    const collisions = await db.user.findMany({
      where: { displayNameLocal: { not: null } },
      select: { id: true, displayNameLocal: true },
    });
    const taken = collisions.some(
      (u) =>
        u.id !== user.id &&
        (u.displayNameLocal ?? "").toLowerCase() === desiredName.toLowerCase(),
    );
    if (taken) {
      throw new Error("That username is already taken.");
    }

    let uploadedUrl: string | null = null;
    if (input.profileImageBase64) {
      try {
        uploadedUrl = await upload({
          bufferOrBase64: input.profileImageBase64,
          fileName: `profile-${user.id}.png`,
        });
      } catch (_e) {
        /* eslint-disable-line @typescript-eslint/no-unused-vars */
        console.error("profile image upload failed", _e);
      }
    }

    const now = new Date();

    const updated = await db.user.update({
      where: { id: user.id },
      data: {
        displayNameLocal: desiredName,
        profileImageUrl: uploadedUrl ?? undefined,
        termsAcceptedAt: user.termsAcceptedAt ?? now,
        privacyAcceptedAt: user.privacyAcceptedAt ?? now,
        ageConfirmedAt: user.ageConfirmedAt ?? now,
        onboardingCompletedAt: user.onboardingCompletedAt ?? now,
      },
      select: {
        id: true,
        displayNameLocal: true,
        profileImageUrl: true,
        termsAcceptedAt: true,
        privacyAcceptedAt: true,
        ageConfirmedAt: true,
      },
    });

    return { ok: true as const, user: updated };
  } catch (error) {
    console.error("completeOnboarding error", error);
    throw error;
  }
}

export async function getMyStartAnchor(input?: { accessToken?: string }) {
  const userId = await requireAppUserId({ accessToken: input?.accessToken });
  const user = await ensureUser(userId);
  const x = (user as any).startWX ?? null;
  const y = (user as any).startWY ?? null;
  return x != null && y != null ? { x, y } : null;
}

// World Map: fetch a rectangular chunk of plots
export async function getMapChunk(input: {
  x0: number;
  y0: number;
  width: number;
  height: number;
  accessToken?: string;
  islandId?: string;
}) {
  const x0 = Math.floor(input?.x0 ?? 0);
  const y0 = Math.floor(input?.y0 ?? 0);
  const width = Math.max(1, Math.floor(input?.width ?? 1));
  const height = Math.max(1, Math.floor(input?.height ?? 1));
  try {
    // For guests this stays public, but if we have a Supabase accessToken we
    // can still correctly compute ownedByMe.
    let me: string | null = null;
    if (input?.accessToken) {
      try {
        me = await requireSupabaseUserId(input.accessToken);
      } catch {
        me = null;
      }
    } else {
      const auth = await getAuth();
      me = auth.status === "authenticated" ? auth.userId : null;
    }

    const minX = Math.floor(x0);
    const minY = Math.floor(y0);
    const maxX = minX + Math.max(1, Math.floor(width)) - 1;
    const maxY = minY + Math.max(1, Math.floor(height)) - 1;

    const islandId = input?.islandId ? String(input.islandId) : null;

    const cells: Array<{
      x: number;
      y: number;
      ownerId: string | null;
      ownedByMe: boolean;
      houseType: string;
    }> = [];

    if (islandId) {
      const holdings = await db.islandHolding.findMany({
        where: {
          islandId,
          x: { gte: minX, lte: maxX },
          y: { gte: minY, lte: maxY },
        },
        select: { x: true, y: true, ownerId: true },
      });
      const map = new Map(holdings.map((h) => [`${h.x}:${h.y}`, h.ownerId]));

      for (let y = minY; y <= maxY; y++) {
        for (let x = minX; x <= maxX; x++) {
          const ownerId = (map.get(`${x}:${y}`) as string | undefined) ?? null;
          cells.push({
            x,
            y,
            ownerId,
            ownedByMe: !!me && ownerId === me,
            houseType: "NONE",
          });
        }
      }

      return { cells };
    }

    const plots = await db.plot.findMany({
      where: { x: { gte: minX, lte: maxX }, y: { gte: minY, lte: maxY } },
    });

    for (let y = minY; y <= maxY; y++) {
      for (let x = minX; x <= maxX; x++) {
        const p = plots.find((pl) => pl.x === x && pl.y === y);
        const ownerId = p?.ownerId ?? null;
        cells.push({
          x,
          y,
          ownerId,
          ownedByMe: !!me && ownerId === me,
          houseType: p?.houseType ?? "NONE",
        });
      }
    }

    return { cells };
  } catch (error) {
    console.error("getMapChunk error", error);
    throw error;
  }
}

// Map overview: get world bounds and a few stats
export async function getMapOverview() {
  const min = await db.plot.findFirst({
    orderBy: [{ x: "asc" }, { y: "asc" }],
  });
  const max = await db.plot.findFirst({
    orderBy: [{ x: "desc" }, { y: "desc" }],
  });
  const count = await db.plot.count();
  const snapshot = await getPricingSnapshot();
  const totalOwned = await totalOwnedM2();
  return {
    bounds:
      min && max
        ? { minX: min.x, minY: min.y, maxX: max.x, maxY: max.y }
        : null,
    plotsCount: count,
    currentPrice: snapshot.currentPrice,
    availableSupplyM2: snapshot.totalSupplyM2 - totalOwned,
  };
}

// Buy and claim specific free plots (each = 1 m²)
export async function buyAndClaimPlots(input: {
  coords: Array<{ x: number; y: number }>;
  islandId?: string;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);

    const coordsUnique = Array.from(
      new Map(
        input.coords.map((c) => [
          c.x + ":" + c.y,
          { x: Math.floor(c.x), y: Math.floor(c.y) },
        ]),
      ).values(),
    );
    if (coordsUnique.length === 0) {
      throw new Error("No cells selected.");
    }

    // Server-side guard deaktiviert: gesamte Welt ist kaufbar

    const isIslandBuy = !!input.islandId;

    // Map-Käufe: echte Welt-Plots prüfen und reservieren
    let freeExisting: any[] = [];
    let toCreate: Array<{ x: number; y: number }> = [];

    if (!isIslandBuy) {
      // Check which are already owned
      const existing = await db.plot.findMany({
        where: { OR: coordsUnique.map((c) => ({ x: c.x, y: c.y })) },
      });

      const existingMap = new Map(existing.map((p) => [p.x + ":" + p.y, p]));
      const alreadyOwned = existing.filter((p) => !!p.ownerId);
      if (alreadyOwned.length > 0) {
        // If any are owned by someone (including me), we reject - keep it simple for now
        throw new Error(
          "Some of the requested m² are no longer available. Please try again.",
        );
      }

      freeExisting = existing.filter((p) => !p.ownerId);
      toCreate = coordsUnique.filter((c) => !existingMap.has(c.x + ":" + c.y));
    } else {
      // Island buys: make sure none of these collectible m² already exist / are owned
      const existingHoldings = await db.islandHolding.findMany({
        where: {
          islandId: String(input.islandId),
          OR: coordsUnique.map((c) => ({ x: c.x, y: c.y })),
        },
        select: { id: true },
        take: 1,
      });
      if (existingHoldings.length > 0) {
        throw new Error(
          "Some of the requested m² are no longer available. Please try again.",
        );
      }
    }

    const nCells = coordsUnique.length;

    // Important distinction:
    // - Global map: each cell can represent multiple m² (scale factor)
    // - Islands: each selected cell is exactly 1 collectible m²
    const scale = isIslandBuy ? 1 : await getCellToM2ScaleInternal();
    const n = isIslandBuy ? nCells : nCells * scale; // actual m²

    const snapshot = await getPricingSnapshot();

    // Check supply (global world supply) – nur für den globalen Map-Markt.
    if (!isIslandBuy) {
      const ownedAll = await totalOwnedM2();
      const available = snapshot.totalSupplyM2 - ownedAll;
      if (n > available + 1e-6) {
        throw new Error("Not enough land available.");
      }
    }
    let stepped: { totalCost: number; avgPrice: number };
    let islandForPricing: any = null;
    let soldBeforeForIsland = 0;
    let islandSupplyLimit = 0;

    if (isIslandBuy) {
      islandForPricing = await db.island.findUnique({
        where: { id: input.islandId! },
      });
      if (!islandForPricing) {
        throw new Error("Island not found.");
      }

      // Treat soldM2/totalSupplyM2 as whole m² (integers). Using floats here can cause
      // off-by-one issues after rounding.
      soldBeforeForIsland = Math.max(
        0,
        Math.floor(Number((islandForPricing as any).soldM2 ?? 0)),
      );
      islandSupplyLimit = Math.max(
        0,
        Math.floor(Number((islandForPricing as any).totalSupplyM2 ?? 0)),
      );
      if (
        islandSupplyLimit > 0 &&
        soldBeforeForIsland + n > islandSupplyLimit
      ) {
        throw new Error("Not enough island m² available.");
      }

      // Aktueller Inselpreis wird deterministisch aus (Startpreis, StepUp%, verkaufte m², totalSupply) berechnet
      const currentPrice = _islandPriceAtSoldM2(
        {
          startPriceUSD: Number(islandForPricing.startPriceUSD ?? 0),
          stepUpPct: Number((islandForPricing as any).stepUpPct ?? 0),
          totalSupplyM2: Number((islandForPricing as any).totalSupplyM2 ?? 1),
        },
        soldBeforeForIsland,
      );

      const totalCost = toCHF(currentPrice * n);
      stepped = {
        totalCost,
        avgPrice: currentPrice,
      };
    } else {
      const pricing = await getCurrentCurvingPriceAndMaybeTrigger();
      stepped = computeDynamicBuyCost({
        amountM2: n,
        currentPrice: pricing.price,
      });
    }

    if (user.balance < stepped.totalCost) {
      throw new Error("Insufficient balance.");
    }

    // Apply updates (atomic + concurrency-safe)
    const updatedUser = await (async () => {
      const affected = isIslandBuy
        ? await db.$executeRaw`
            UPDATE User
            SET
              balance = ROUND(COALESCE(balance, 0) - ${stepped.totalCost}, 2),
              landOwnedM2 = COALESCE(landOwnedM2, 0) + ${n}
            WHERE id = ${user.id}
              AND COALESCE(balance, 0) >= ${stepped.totalCost}
          `
        : await db.$executeRaw`
            UPDATE User
            SET
              balance = ROUND(COALESCE(balance, 0) - ${stepped.totalCost}, 2),
              landOwnedM2 = COALESCE(landOwnedM2, 0) + ${n},
              mainMarketPurchasedM2 = COALESCE(mainMarketPurchasedM2, 0) + ${n}
            WHERE id = ${user.id}
              AND COALESCE(balance, 0) >= ${stepped.totalCost}
          `;
      if (!affected) throw new Error("Insufficient balance.");
      const u = await db.user.findUnique({ where: { id: user.id } });
      if (!u) throw new Error("User not found");
      return u;
    })();

    // Areas disabled: no area tracking

    if (!isIslandBuy) {
      const marketRow = await ensureMarket();
      const current = (marketRow as any).currentPrice ?? snapshot.currentPrice;
      const newPrice = toCHF(current * Math.pow(1 + 0.004, n));
      await db.market.update({
        where: { id: marketRow.id },
        data: {
          cumulativeSoldM2: snapshot.cumulativeSoldM2 + n,
          pressureUpdatedAt: new Date(),
          currentPrice: newPrice,
        },
      });

      // Record global price history only (islands have their own independent pricing).
      await _recordMarketPriceSamples({
        startPrice: current,
        amountM2: n,
        kind: "BUY",
      });
    } else if (islandForPricing) {
      // Insel-spezifische Bonding-Curve:
      // Der Creator wählt max. +5%, die tatsächliche Steigung skaliert aber mathematisch mit der verkauften m²‑Menge.
      const soldBefore = soldBeforeForIsland;
      const soldAfter = soldBefore + n;

      const newPrice = _islandPriceAtSoldM2(
        {
          startPriceUSD: Number(islandForPricing.startPriceUSD ?? 0),
          stepUpPct: Number((islandForPricing as any).stepUpPct ?? 0),
          totalSupplyM2: Number((islandForPricing as any).totalSupplyM2 ?? 1),
        },
        soldAfter,
      );

      try {
        await db.island.update({
          where: { id: islandForPricing.id },
          data: {
            soldM2: soldAfter,
          },
        });
      } catch (err) {
        console.error(
          "update island soldM2 (buyAndClaimPlots island) failed",
          err,
        );
      }
      try {
        await db.islandPriceSample.create({
          data: {
            islandId: islandForPricing.id,
            priceUSD: newPrice,
          },
        });
      } catch (err) {
        console.error(
          "islandPriceSample create (buyAndClaimPlots island) failed",
          err,
        );
      }
    }

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "BUY",
        m2: n,
        avgPricePerM2: stepped.avgPrice,
        feePct: 0,
        totalAmountCHF: stepped.totalCost,
        meta: JSON.stringify({
          via: isIslandBuy ? "island" : "map",
          cells: n,
          islandId: input.islandId ?? null,
        }),
      },
    });

    // Update demand-driven counters
    await onBuyCommitted({ buyerId: user.id, amountM2: n });

    // Map-Kauf: echte Plots + Zertifikate + Realtime-Update
    if (!isIslandBuy) {
      // Create new plots for coords not present yet
      const createdPlots = await Promise.all(
        toCreate.map((c) =>
          db.plot.create({
            data: { x: c.x, y: c.y, ownerId: user.id, houseType: "NONE" },
          }),
        ),
      );

      // Claim existing free plots by updating ownerId
      const updatedPlots = await Promise.all(
        freeExisting.map((p) =>
          db.plot.update({ where: { id: p.id }, data: { ownerId: user.id } }),
        ),
      );

      const allPurchasedPlots = [...createdPlots, ...updatedPlots];

      // Create one certificate per purchased plot with purchasedAt now
      try {
        await db.certificate.createMany({
          data: allPurchasedPlots.map((p) => ({
            userId: user.id,
            plotId: p.id,
            purchasedAt: new Date(),
          })),
        });
      } catch (_e) {
        /* eslint-disable-line @typescript-eslint/no-unused-vars */
        console.error("certificate createMany (map buy) failed", _e);
      }

      // Realtime: broadcast changed plots so all clients update instantly
      try {
        await setRealtimeStore({
          channelId: "map",
          data: {
            ts: Date.now(),
            changes: allPurchasedPlots.map((p) => ({
              x: p.x,
              y: p.y,
              ownerId: user.id,
              houseType: "NONE",
            })),
          },
        });
      } catch (_e) {
        /* eslint-disable-line @typescript-eslint/no-unused-vars */
        console.error("broadcast map (buy) failed", _e);
      }
    }

    // Islands: persist ownership for each purchased collectible m²
    if (isIslandBuy && islandForPricing) {
      try {
        await db.islandHolding.createMany({
          data: coordsUnique.map((c) => ({
            islandId: islandForPricing.id,
            x: c.x,
            y: c.y,
            ownerId: user.id,
          })),
        });
      } catch (err) {
        console.error(
          "islandHolding createMany (buyAndClaimPlots island) failed",
          err,
        );
      }

      try {
        await setRealtimeStore({
          channelId: "map",
          data: {
            ts: Date.now(),
            changes: coordsUnique.map((c) => ({
              x: c.x,
              y: c.y,
              ownerId: user.id,
              houseType: "NONE",
            })),
          },
        });
      } catch {
        // ignore
      }
    }

    await broadcastMarket();

    return {
      ok: true as const,
      purchased: n,
      userBalanceCHF: toCHF(updatedUser.balance),
    };
  } catch (error) {
    const msg = (error as any)?.message ?? String(error);
    const expectedMsgs = [
      "Invalid amount",
      "Not enough land available",
      "No free cells available",
      "Insufficient balance",
      "Some of the requested m² are no longer available",
      "Purchase is only allowed inside the island",
    ];
    if (!expectedMsgs.some((m) => msg.includes(m))) {
      console.error("buyAndClaimPlots error", error);
    }
    throw error;
  }
}

// Quick Buy: automatically pick free plots near the user's start and purchase them
export async function quickBuy(input: { m2: number; accessToken?: string }) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);

    const requested = Math.floor(input?.m2 ?? 0);
    if (!Number.isFinite(requested) || requested < 1) {
      throw new Error("Invalid amount (m²). Minimum is 1 m².");
    }

    // Clamp to available supply
    const snapshot = await getPricingSnapshot();
    const ownedAll = await totalOwnedM2();
    const available = snapshot.totalSupplyM2 - ownedAll;
    if (available <= 0) throw new Error("Not enough land available.");
    const toBuy = Math.min(requested, Math.floor(available));

    // Determine a starting point (unique per user) and pick free cells around it
    const startX =
      (user as any).startWX ?? ISLAND_X0 + Math.floor(ISLAND_W / 2);
    const startY =
      (user as any).startWY ?? ISLAND_Y0 + Math.floor(ISLAND_H / 2);

    const coords: Array<{ x: number; y: number }> = [];
    const seen = new Set<string>();

    // Helper to test if a world cell is free and allowed
    const isFreeAllowed = async (x: number, y: number) => {
      if (!_isAllowedLot(x, y)) return false;
      const key = `${x}:${y}`;
      if (seen.has(key)) return false;
      const existing = await db.plot.findFirst({ where: { x, y } });
      if (!existing) return true;
      return !existing.ownerId; // free if exists without owner
    };

    const maxRadius = Math.max(ISLAND_W, ISLAND_H);
    // Always include the start cell first if it's free and allowed
    if (coords.length < toBuy && (await isFreeAllowed(startX, startY))) {
      coords.push({ x: startX, y: startY });
      seen.add(`${startX}:${startY}`);
    }

    // Expand in a spiral/rings until enough coordinates are found
    outer: for (let r = 1; r < maxRadius && coords.length < toBuy; r++) {
      // top and bottom rows of the ring
      for (let dx = -r; dx <= r && coords.length < toBuy; dx++) {
        const x1 = startX + dx;
        const yTop = startY - r;
        const yBot = startY + r;
        if (await isFreeAllowed(x1, yTop)) {
          coords.push({ x: x1, y: yTop });
          seen.add(`${x1}:${yTop}`);
        }
        if (coords.length >= toBuy) break outer;
        if (await isFreeAllowed(x1, yBot)) {
          coords.push({ x: x1, y: yBot });
          seen.add(`${x1}:${yBot}`);
        }
      }
      // left and right columns of the ring (excluding corners already checked)
      for (let dy = -r + 1; dy <= r - 1 && coords.length < toBuy; dy++) {
        const y1 = startY + dy;
        const xL = startX - r;
        const xR = startX + r;
        if (await isFreeAllowed(xL, y1)) {
          coords.push({ x: xL, y: y1 });
          seen.add(`${xL}:${y1}`);
        }
        if (coords.length >= toBuy) break outer;
        if (await isFreeAllowed(xR, y1)) {
          coords.push({ x: xR, y: y1 });
          seen.add(`${xR}:${y1}`);
        }
      }
    }

    if (coords.length === 0) {
      throw new Error("No free cells found nearby.");
    }

    // Delegate purchase + placement to existing server-authoritative flow
    const res = await buyAndClaimPlots({
      coords,
      accessToken: input?.accessToken,
    });
    return { ...res, placed: coords };
  } catch (error) {
    const msg = (error as any)?.message ?? String(error);
    const expectedMsgs = [
      "Invalid amount",
      "Not enough land available",
      "No free cells available",
      "Insufficient balance",
      "Some of the requested m² are no longer available",
      "Purchase is only allowed inside the island",
    ];
    if (!expectedMsgs.some((m) => msg.includes(m))) {
      console.error("quickBuy error", error);
    }
    throw error;
  }
}

// Place/upgrade a house on an owned plot
export async function placeHouseOnPlot(input: {
  x: number;
  y: number;
  houseType: "BASIC" | "VILLA" | "MANSION";
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);

    const { x, y, houseType } = input;

    const anchorX = Math.floor(x);
    const anchorY = Math.floor(y);

    const typeNormalized = houseType;

    const SIZE: Record<string, number> = {
      BASIC: 8,
      VILLA: 19,
      MANSION: 36,
    };

    const requiredCells = SIZE[typeNormalized];
    if (!requiredCells) throw new Error("Invalid house type.");
    if (requiredCells > 36) {
      throw new Error("A maximum of 36 m² per build is allowed.");
    }

    if (typeNormalized === "MANSION") {
      const now = new Date();
      const available = now.getMinutes() % 2 === 0;
      if (!available) {
        throw new Error("Not available right now.");
      }
    }

    const pricing = await getCurrentCurvingPriceAndMaybeTrigger();
    const scale = await getCellToM2ScaleInternal();

    // 5% des aktuellen Preises pro m²‑Äquivalent
    const unitFactor = 0.05;
    const buildCost = toCHF(pricing.price * requiredCells * scale * unitFactor);
    if (user.balance < buildCost) {
      throw new Error("Insufficient balance for building.");
    }

    // Anchor muss eigene, freie Zelle sein
    const anchorPlot = await db.plot.findFirst({
      where: { x: anchorX, y: anchorY },
    });
    if (!anchorPlot || anchorPlot.ownerId !== user.id) {
      throw new Error("You do not own the anchor plot.");
    }
    if (
      !_islandContains(anchorX, anchorY) ||
      _isProtectedPlot(anchorX, anchorY) ||
      !_isAllowedLot(anchorX, anchorY)
    ) {
      throw new Error("You can only build on the island.");
    }
    if (anchorPlot.houseType && anchorPlot.houseType !== "NONE") {
      throw new Error("The anchor tile is already built.");
    }

    // Finde beliebige freien eigenen Zellen (Form egal), wähle die nächsten zum Anker
    let candidates = await db.plot.findMany({
      where: { ownerId: user.id, houseType: "NONE" },
      take: 1000, // Sicherheitslimit
    });
    // Zulassen auf Sand UND Land (nur innerhalb der Insel, nicht geschützt)
    candidates = candidates.filter(
      (c) => _isAllowedLot(c.x, c.y) && !_isProtectedPlot(c.x, c.y),
    );

    // Sicherstellen, dass die Ankerzelle enthalten ist
    const pool = [
      anchorPlot,
      ...candidates.filter((c) => c.id !== anchorPlot.id),
    ];

    // Nach Distanz zum Anker sortieren und die ersten requiredCells auswählen
    const sorted = pool
      .map((p) => ({
        p,
        d:
          (p.x - anchorX) * (p.x - anchorX) + (p.y - anchorY) * (p.y - anchorY),
      }))
      .sort((a, b) => a.d - b.d)
      .map((x) => x.p);

    if (sorted.length < requiredCells) {
      throw new Error(`You need ${requiredCells} free owned tiles nearby.`);
    }

    const toUse = sorted.slice(0, requiredCells);

    // Abbuchen & Transaktion erfassen
    await db.user.update({
      where: { id: user.id },
      data: { balance: toCHF(user.balance - buildCost) },
    });

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "BUILD",
        m2: requiredCells,
        avgPricePerM2: 0,
        feePct: 0,
        totalAmountCHF: buildCost,
        meta: JSON.stringify({
          x: anchorX,
          y: anchorY,
          houseType: typeNormalized,
          consumedCells: requiredCells,
        }),
      },
    });

    // Markiere ausgewählte Zellen als bebaut (Form egal)
    await Promise.all(
      toUse.map((p) =>
        db.plot.update({
          where: { id: p.id },
          data: { houseType: typeNormalized },
        }),
      ),
    );

    // Realtime: broadcast house placement to all clients
    try {
      await setRealtimeStore({
        channelId: "map",
        data: {
          ts: Date.now(),
          changes: toUse.map((p) => ({
            x: p.x,
            y: p.y,
            ownerId: user.id,
            houseType: typeNormalized,
          })),
        },
      });
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("broadcast map (placeHouseOnPlot) failed", _e);
    }

    return { ok: true as const };
  } catch (error) {
    console.error("placeHouseOnPlot error", error);
    throw error;
  }
}

// Sell house cluster by tapping any of its cells (refund portion of build cost)
export async function sellHouseAtPlot(): Promise<never> {
  // Haus‑Einzelverkauf ist deaktiviert: nur Grundstücke (inkl. evtl. Haus) können verkauft werden
  throw new Error(
    "Selling a house by itself is disabled. Please sell the plot instead.",
  );
}

// Market status: price, available supply, totals
export async function getLandingAssets() {
  try {
    const market = await ensureMarket();
    return {
      mapBackgroundUrl: market.mapBackgroundUrl ?? null,
      coinImageUrl: (market as any).coinImageUrl ?? null,
      landingImageUrl: (market as any).landingImageUrl ?? null,
      landingImageUrl2: (market as any).landingImageUrl2 ?? null,
      landingImageUrl3: (market as any).landingImageUrl3 ?? null,
      landingImageUrl4: (market as any).landingImageUrl4 ?? null,
      landingSelectSecondMarketImageUrl:
        (market as any).landingSelectSecondMarketImageUrl ?? null,
      landingSelectWalletImageUrl:
        (market as any).landingSelectWalletImageUrl ?? null,
      landingImageUrl5: (market as any).landingImageUrl5 ?? null,
      landingIslandsImageUrl: (market as any).landingIslandsImageUrl ?? null,
      bottomLandingImageUrl: (market as any).bottomLandingImageUrl ?? null,
    };
  } catch (error) {
    console.error("getLandingAssets error", error);
    throw error;
  }
}

export async function getMarket() {
  try {
    const snapshot = await getPricingSnapshot();
    const owned = await totalOwnedM2();
    const available = snapshot.totalSupplyM2 - owned;
    const market = await ensureMarket();
    return {
      pricePerM2CHF: toCHF(snapshot.currentPrice),
      availableSupplyM2: available,
      totalSupplyM2: snapshot.totalSupplyM2,
      cumulativeSoldM2: snapshot.cumulativeSoldM2,
      stepSizeM2: snapshot.stepSizeM2,
      stepIncreasePct: 0,
      sellFeePct: snapshot.sellFeePct,
      basePrice: snapshot.basePrice,
      certificateTemplateUrl: snapshot.certificateTemplateUrl,
      mapBackgroundUrl: market.mapBackgroundUrl ?? null,
      coinImageUrl: (market as any).coinImageUrl ?? null,
      landingImageUrl: (market as any).landingImageUrl ?? null,
      landingImageUrl2: (market as any).landingImageUrl2 ?? null,
      landingImageUrl3: (market as any).landingImageUrl3 ?? null,
      landingImageUrl4: (market as any).landingImageUrl4 ?? null,
      landingSelectSecondMarketImageUrl:
        (market as any).landingSelectSecondMarketImageUrl ?? null,
      landingSelectWalletImageUrl:
        (market as any).landingSelectWalletImageUrl ?? null,
      landingImageUrl5: (market as any).landingImageUrl5 ?? null,
      landingIslandsImageUrl: (market as any).landingIslandsImageUrl ?? null,
      bottomLandingImageUrl: (market as any).bottomLandingImageUrl ?? null,
    };
  } catch (error) {
    console.error("getMarket error", error);
    throw error;
  }
}

export async function getMarketStats() {
  try {
    const snapshot = await getPricingSnapshot();
    const owned = await totalOwnedM2();
    const availableSupplyM2 = snapshot.totalSupplyM2 - owned;

    const since = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const [usersAgg, volAgg, athAgg] = await Promise.all([
      db.user.aggregate({
        _sum: { balance: true },
      }),
      db.transaction.aggregate({
        where: {
          createdAt: { gte: since },
          type: { in: ["BUY", "SELL", "SECONDARY_BUY", "SECONDARY_SOLD"] },
        },
        _sum: { totalAmountCHF: true },
      }),
      db.marketPriceSample.aggregate({
        _max: { price: true },
      }),
    ]);

    const liquidityPPC = toCHF(Number((usersAgg as any)?._sum?.balance ?? 0));
    const volume24hPPC = toCHF(
      Number((volAgg as any)?._sum?.totalAmountCHF ?? 0),
    );

    const current = toCHF(snapshot.currentPrice);
    const athPricePerM2PPC = toCHF(
      Number((athAgg as any)?._max?.price ?? current),
    );

    return {
      updatedAt: new Date(),
      availableSupplyM2,
      liquidityPPC,
      volume24hPPC,
      athPricePerM2PPC,
    };
  } catch (error) {
    console.error("getMarketStats error", error);
    throw error;
  }
}

// Admin: set the exact map background image (photo) to be used as the base layer
export async function setMapBackgroundImage(input: {
  base64: string;
  fileName?: string;
}) {
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const me = await ensureUser(auth.userId);
  if (!me.isAdmin) {
    throw new Error("Not authorized");
  }
  const url = await upload({
    bufferOrBase64: input.base64,
    fileName: input.fileName ?? `map-background-${Date.now()}.png`,
  });
  const market = await ensureMarket();
  await db.market.update({
    where: { id: market.id },
    data: { mapBackgroundUrl: url },
  });
  return { url };
}

// Admin: set PP15 coin image used in 2nd Market listings
export async function setCoinImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const me = await ensureUser(auth.userId);
  if (!me.isAdmin) throw new Error("Admins only.");
  const url = await upload({
    bufferOrBase64: input.base64,
    fileName: input.fileName ?? `pp15-coin-${Date.now()}.png`,
  });
  const market = await ensureMarket();
  await db.market.update({
    where: { id: market.id },
    data: { coinImageUrl: url },
  });
  return { ok: true as const, url };
}

export async function setLandingImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setBottomLandingImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingImage2FromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingImage3FromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingImage4FromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingImage5FromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingSelectSecondMarketImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function setLandingSelectWalletImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

export async function adminSaveLandingImages(input: {
  landingImageUrl?: string | null;
  landingImageUrl2?: string | null;
  landingImageUrl3?: string | null;
  landingImageUrl4?: string | null;
  landingImageUrl5?: string | null;
  landingSelectSecondMarketImageUrl?: string | null;
  landingSelectWalletImageUrl?: string | null;
  landingIslandsImageUrl?: string | null;
  bottomLandingImageUrl?: string | null;
  mapBackgroundUrl?: string | null;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
    skippedKeys: [] as string[],
  };
}

export async function setLandingIslandsImageFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  void input;
  // Locked: landing images can no longer be changed from inside the app.
  return {
    ok: false as const,
    message: "Landing images are locked and cannot be changed.",
  };
}

// Simulate buy cost (server-authoritative preview)
export async function simulateBuyCost({ m2 }: { m2: number }) {
  try {
    const snapshot = await getPricingSnapshot();
    if (!Number.isFinite(m2) || m2 < 1) {
      return {
        totalCostCHF: 0,
        avgPriceCHF: snapshot.currentPrice,
        stepsCrossed: 0,
      };
    }
    const owned = await totalOwnedM2();
    const available = snapshot.totalSupplyM2 - owned;
    const clamped = Math.min(Math.floor(m2), available);
    if (clamped <= 0) {
      return {
        totalCostCHF: 0,
        avgPriceCHF: snapshot.currentPrice,
        stepsCrossed: 0,
      };
    }
    const calc = computeDynamicBuyCost({
      amountM2: clamped,
      currentPrice: snapshot.currentPrice,
    });
    return {
      totalCostCHF: calc.totalCost,
      avgPriceCHF: calc.avgPrice,
      stepsCrossed: 0,
      clampedM2: clamped,
    };
  } catch (error) {
    console.error("simulateBuyCost error", error);
    throw error;
  }
}

// Simulate sell proceeds (server-authoritative preview)
export async function simulateSellProceeds({ m2 }: { m2: number }) {
  try {
    const snapshot = await getPricingSnapshot();
    const auth = await getAuth();
    let maxSell = 0;
    if (auth.status === "authenticated") {
      const user = await ensureUserWithCompliance(auth.userId);
      maxSell = user.landOwnedM2;
    }
    if (!Number.isFinite(m2) || m2 <= 0) {
      return { grossCHF: 0, feeCHF: 0, netCHF: 0, clampedM2: 0 };
    }
    const clamped = Math.min(m2, maxSell);
    const scalc = computeDynamicSellProceeds({
      amountM2: clamped,
      currentPrice: snapshot.currentPrice,
    });
    const fee = (snapshot.sellFeePct / 100) * scalc.gross;
    const net = scalc.gross - fee;
    return {
      grossCHF: toCHF(scalc.gross),
      feeCHF: toCHF(fee),
      netCHF: toCHF(net),
      clampedM2: clamped,
    };
  } catch (error) {
    console.error("simulateSellProceeds error", error);
    throw error;
  }
}

export async function getHoldingsBreakdown(input: {
  accessToken?: string;
  deviceId?: string;
}): Promise<HoldingsBreakdown> {
  try {
    const userId = input?.accessToken
      ? await requireSupabaseUserId(input.accessToken)
      : requireGuestDeviceId(input?.deviceId);

    await ensureUserWithCompliance(userId);

    const txs = await db.transaction.findMany({
      where: { userId },
      orderBy: { createdAt: "asc" },
      select: { type: true, m2: true },
    });

    type Source = "MAIN" | "SECONDARY";
    type Lot = { source: Source; remaining: number };
    const inventory: Lot[] = [];

    const addLot = (source: Source, units: number) => {
      const u = Math.max(0, Math.floor(units));
      if (u <= 0) return;
      inventory.push({ source, remaining: u });
    };

    const consume = (unitsToSell: number) => {
      let remaining = Math.max(0, Math.floor(unitsToSell));
      while (remaining > 0 && inventory.length > 0) {
        const lot = inventory[0]!;
        const take = Math.min(lot.remaining, remaining);
        lot.remaining -= take;
        remaining -= take;
        if (lot.remaining <= 0) inventory.shift();
      }
    };

    for (const tx of txs) {
      const type = String(tx.type || "").toUpperCase();

      if (type === "BUY") {
        addLot("MAIN", tx.m2 ?? 0);
        continue;
      }
      if (type === "SECONDARY_BUY") {
        addLot("SECONDARY", tx.m2 ?? 1);
        continue;
      }
      if (type === "SELL" || type === "SECONDARY_SOLD") {
        consume(tx.m2 ?? 0);
        continue;
      }
    }

    const mainM2 = inventory
      .filter((l) => l.source === "MAIN")
      .reduce((acc, l) => acc + l.remaining, 0);
    const secondaryM2 = inventory
      .filter((l) => l.source === "SECONDARY")
      .reduce((acc, l) => acc + l.remaining, 0);
    const totalM2 = mainM2 + secondaryM2;

    return { totalM2, mainM2, secondaryM2 };
  } catch (error) {
    console.error("getHoldingsBreakdown error", error);
    throw error;
  }
}

export async function getIslandHoldingsValuation(input: {
  accessToken: string;
}): Promise<{
  totalIslandM2: number;
  estimatedValueUSD: number;
  perIsland: Array<{
    islandId: string;
    islandName: string;
    ticker: string;
    m2: number;
    priceUSD: number;
    valueUSD: number;
  }>;
}> {
  try {
    const userId = await requireSupabaseUserId(input?.accessToken);
    await ensureUserWithCompliance(userId);

    const grouped = await db.islandHolding.groupBy({
      by: ["islandId"],
      where: { ownerId: userId },
      _count: { _all: true },
    });

    const ids = grouped.map((g) => String(g.islandId));
    if (!ids.length) {
      return { totalIslandM2: 0, estimatedValueUSD: 0, perIsland: [] };
    }

    const islands = await db.island.findMany({
      where: { id: { in: ids } },
      select: {
        id: true,
        name: true,
        ticker: true,
        startPriceUSD: true,
        stepUpPct: true,
        totalSupplyM2: true,
        soldM2: true,
      },
    });

    const islandMap = new Map(islands.map((i) => [String(i.id), i]));

    const perIsland = grouped
      .map((g) => {
        const islandId = String(g.islandId);
        const island = islandMap.get(islandId);
        const m2 = Math.max(
          0,
          Math.floor(Number((g as any)?._count?._all ?? 0)),
        );
        if (!island || m2 <= 0) return null;

        const sold = Math.max(
          0,
          Math.floor(Number((island as any).soldM2 ?? 0)),
        );
        const priceUSD = _islandPriceAtSoldM2(
          {
            startPriceUSD: Number((island as any).startPriceUSD ?? 0),
            stepUpPct: Number((island as any).stepUpPct ?? 0),
            totalSupplyM2: Number((island as any).totalSupplyM2 ?? 1),
          },
          sold,
        );

        const valueUSD = Math.max(0, m2 * (Number(priceUSD) || 0));

        return {
          islandId,
          islandName: String((island as any).name ?? "Island"),
          ticker: String((island as any).ticker ?? ""),
          m2,
          priceUSD: Number(priceUSD) || 0,
          valueUSD,
        };
      })
      .filter(Boolean) as Array<{
      islandId: string;
      islandName: string;
      ticker: string;
      m2: number;
      priceUSD: number;
      valueUSD: number;
    }>;

    const totalIslandM2 = perIsland.reduce((acc, r) => acc + (r.m2 || 0), 0);
    const estimatedValueUSD = perIsland.reduce(
      (acc, r) => acc + (r.valueUSD || 0),
      0,
    );

    return {
      totalIslandM2,
      estimatedValueUSD: toCHF(estimatedValueUSD),
      perIsland,
    };
  } catch (error) {
    console.error("getIslandHoldingsValuation error", error);
    throw error;
  }
}

// Portfolio summary for current user
export async function getPortfolio(input?: {
  accessToken?: string;
}): Promise<PortfolioSummary> {
  try {
    const userId = await requireSupabaseUserId(input?.accessToken);
    const user = await ensureUserWithCompliance(userId);
    const snapshot = await getPricingSnapshot();
    const houses = await getUserHouses(userId);
    const assigned = computeAssignedM2(houses);
    const unassigned = Math.max(0, user.landOwnedM2 - assigned);
    const estimatedValueCHF = computeEstimatedValueCHF({
      landOwnedM2: user.landOwnedM2,
      houses,
      currentPrice: snapshot.currentPrice,
    });

    return {
      userId: user.id,
      balance: toCHF(user.balance),
      landOwnedM2: user.landOwnedM2,
      unassignedM2: unassigned,
      houses,
      estimatedValueCHF,
    };
  } catch (error) {
    console.error("getPortfolio error", error);
    throw error;
  }
}

export async function getGuestPortfolio(input: { deviceId: string }) {
  try {
    const userId = requireGuestDeviceId(input?.deviceId);
    const user = await ensureUserWithCompliance(userId);
    const snapshot = await getPricingSnapshot();
    const houses = await getUserHouses(userId);
    const assigned = computeAssignedM2(houses);
    const unassigned = Math.max(0, user.landOwnedM2 - assigned);
    const estimatedValueCHF = computeEstimatedValueCHF({
      landOwnedM2: user.landOwnedM2,
      houses,
      currentPrice: snapshot.currentPrice,
    });

    return {
      userId: user.id,
      balance: toCHF(user.balance),
      landOwnedM2: user.landOwnedM2,
      unassignedM2: unassigned,
      houses,
      estimatedValueCHF,
    };
  } catch (error) {
    console.error("getGuestPortfolio error", error);
    throw error;
  }
}

// Count placed houses by visual type on plots owned by current user
export async function getMyHouseTypeCounts() {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    await ensureUser(auth.userId);

    const [basic, villa, mansion] = await Promise.all([
      db.plot.count({ where: { ownerId: auth.userId, houseType: "BASIC" } }),
      db.plot.count({ where: { ownerId: auth.userId, houseType: "VILLA" } }),
      db.plot.count({ where: { ownerId: auth.userId, houseType: "MANSION" } }),
    ]);

    return {
      basic,
      villa,
      mansion,
      totalHouses: basic + villa + mansion,
    };
  } catch (error) {
    console.error("getMyHouseTypeCounts error", error);
    throw error;
  }
}

// Buy land (server-authoritative, step-based pricing)
export async function buyLand({
  m2,
  accessToken,
}: {
  m2: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken });
    const user = await ensureUserWithCompliance(userId);
    const snapshot = await getPricingSnapshot();

    const requestM2 = Math.floor(m2);
    if (!Number.isFinite(requestM2) || requestM2 < 1) {
      throw new Error("Invalid amount (m²). Minimum is 1 m².");
    }

    const ownedAll = await totalOwnedM2();
    const available = snapshot.totalSupplyM2 - ownedAll;
    if (requestM2 > available) {
      throw new Error("Not enough land available.");
    }

    const stepped = computeDynamicBuyCost({
      amountM2: requestM2,
      currentPrice: snapshot.currentPrice,
    });

    if (user.balance < stepped.totalCost) {
      throw new Error("Insufficient balance.");
    }

    // Apply updates
    const initialOwned = user.landOwnedM2;

    const updatedUser = await (async () => {
      const affected = await db.$executeRaw`
        UPDATE User
        SET
          balance = ROUND(COALESCE(balance, 0) - ${stepped.totalCost}, 2),
          landOwnedM2 = COALESCE(landOwnedM2, 0) + ${requestM2},
          mainMarketPurchasedM2 = COALESCE(mainMarketPurchasedM2, 0) + ${requestM2}
        WHERE id = ${user.id}
          AND COALESCE(balance, 0) >= ${stepped.totalCost}
      `;
      if (!affected) throw new Error("Insufficient balance.");
      const u = await db.user.findUnique({ where: { id: user.id } });
      if (!u) throw new Error("User not found");
      return u;
    })();

    // Create certificates; when this is the user's first purchase, also auto-assign
    // exactly one free plot inside the island and tie a certificate to it.
    try {
      const now = new Date();
      if (initialOwned === 0 && requestM2 >= 1) {
        const anchor = await _pickFirstFreePlotAnchor();
        const plot = await db.plot.create({
          data: {
            x: anchor.x,
            y: anchor.y,
            ownerId: user.id,
            houseType: "NONE",
          },
        });
        // one certificate linked to the new plot
        await db.certificate.create({
          data: { userId: user.id, plotId: plot.id, purchasedAt: now },
        });
        // remaining certificates without plot
        const remaining = requestM2 - 1;
        if (remaining > 0) {
          await db.certificate.createMany({
            data: Array.from({ length: remaining }).map(() => ({
              userId: user.id,
              purchasedAt: now,
            })),
          });
        }
        // notify map subscribers so the tile appears immediately
        try {
          await setRealtimeStore({
            channelId: "map",
            data: {
              ts: Date.now(),
              changes: [
                { x: plot.x, y: plot.y, ownerId: user.id, houseType: "NONE" },
              ],
            },
          });
        } catch (_e) {
          /* eslint-disable-line @typescript-eslint/no-unused-vars */
          console.error("broadcast map (buyLand auto anchor) failed", _e);
        }
      } else {
        // generic certificates only
        await db.certificate.createMany({
          data: Array.from({ length: requestM2 }).map(() => ({
            userId: user.id,
            purchasedAt: now,
          })),
        });
      }
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("certificate createMany (buyLand) failed", _e);
    }

    const marketRow = await ensureMarket();
    const current = (marketRow as any).currentPrice ?? snapshot.currentPrice;
    const newPrice = toCHF(current * Math.pow(1 + 0.004, requestM2));
    await db.market.update({
      where: { id: marketRow.id },
      data: {
        cumulativeSoldM2: snapshot.cumulativeSoldM2 + requestM2,
        pressureUpdatedAt: new Date(),
        currentPrice: newPrice,
      },
    });

    // Record global price history only (islands have their own independent pricing).
    await _recordMarketPriceSamples({
      startPrice: current,
      amountM2: requestM2,
      kind: "BUY",
    });

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "BUY",
        m2: requestM2,
        avgPricePerM2: stepped.avgPrice,
        feePct: 0,
        totalAmountCHF: stepped.totalCost,
        meta: JSON.stringify({
          startedAtPrice: snapshot.currentPrice,
          autoAnchorPlaced: initialOwned === 0 && requestM2 >= 1,
        }),
      },
    });

    await onBuyCommitted({ buyerId: user.id, amountM2: requestM2 });

    await broadcastMarket();

    return {
      ok: true as const,
      portfolio: await getPortfolio(),
      market: await getMarket(),
      userBalanceCHF: toCHF(updatedUser.balance),
    };
  } catch (error) {
    console.error("buyLand error", error);
    throw error;
  }
}

// Sell land (2% fee, free lowest-tier house assignments if needed)
export async function sellLand(input: { m2: number; accessToken?: string }) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);
    const snapshot = await getPricingSnapshot();

    const { m2 } = input;

    const requestM2 = Math.floor(m2);
    if (!Number.isFinite(requestM2) || requestM2 < 1) {
      throw new Error("Invalid amount (m²). Minimum is 1 m².");
    }

    if (requestM2 > user.landOwnedM2) {
      throw new Error("You do not have enough land to sell.");
    }

    const sellCalc = computeDynamicSellProceeds({
      amountM2: requestM2,
      currentPrice: snapshot.currentPrice,
    });
    const gross = sellCalc.gross;
    const fee = toCHF((snapshot.sellFeePct / 100) * gross);
    const net = toCHF(gross - fee);

    // Adjust user land
    const newOwned = user.landOwnedM2 - requestM2;

    // Ensure house assignments fit within remaining land
    const houses = await getUserHouses(user.id);
    let assigned = computeAssignedM2(houses);
    if (assigned > newOwned) {
      // Need to free houses (reduce counts) from lowest bonus level first
      let toFreeM2 = assigned - newOwned;
      let neededHouses = Math.ceil(toFreeM2 / 100);

      for (const lvl of LEVELS_LOW_TO_HIGH) {
        if (neededHouses <= 0) break;
        const canRemove = Math.min(houses[lvl], neededHouses);
        if (canRemove > 0) {
          houses[lvl] -= canRemove;
          neededHouses -= canRemove;
          await db.houseHolding.upsert({
            where: { userId_level: { userId: user.id, level: lvl } },
            update: { count: houses[lvl] },
            create: { userId: user.id, level: lvl, count: houses[lvl] },
          });
        }
      }

      assigned = computeAssignedM2(houses);
      if (assigned > newOwned) {
        // Safety guard (should not happen after reduction)
        throw new Error("Failed to release house allocations.");
      }
    }

    const updatedUser = await (async () => {
      const affected = await db.$executeRaw`
        UPDATE User
        SET
          landOwnedM2 = CASE
            WHEN COALESCE(landOwnedM2, 0) >= ${requestM2} THEN COALESCE(landOwnedM2, 0) - ${requestM2}
            ELSE COALESCE(landOwnedM2, 0)
          END,
          balance = ROUND(COALESCE(balance, 0) + ${net}, 2)
        WHERE id = ${user.id}
          AND COALESCE(landOwnedM2, 0) >= ${requestM2}
      `;
      if (!affected) throw new Error("You do not have enough land to sell.");
      const u = await db.user.findUnique({ where: { id: user.id } });
      if (!u) throw new Error("User not found");
      return u;
    })();

    // Estimate profit/loss versus last BUY avg price per m²
    const lastBuy = await db.transaction.findFirst({
      where: { userId: user.id, type: "BUY" },
      orderBy: { createdAt: "desc" },
      select: { avgPricePerM2: true },
    });
    const lastAvg = lastBuy?.avgPricePerM2 ?? snapshot.currentPrice;
    const profitCHF = toCHF(net - lastAvg * requestM2);

    // Auto payout of profits if enabled
    try {
      const profile = await db.payoutProfile.findUnique({
        where: { userId: user.id },
      });
      if (
        profile?.autoPayoutOnProfit &&
        profile.acceptedPayoutTosAt &&
        profitCHF > 0
      ) {
        const methodSnapshot = JSON.stringify({
          method: profile.method,
          paypalEmail: profile.paypalEmail,
          iban: profile.iban,
          accountHolderName: profile.accountHolderName,
          country: profile.country,
        });
        const amount = Math.min(profitCHF, updatedUser.balance ?? 0);
        if (amount > 0.01) {
          await db.user.update({
            where: { id: user.id },
            data: { balance: toCHF((updatedUser.balance ?? 0) - amount) },
          });
          await db.withdrawalRequest.create({
            data: {
              userId: user.id,
              amountCHF: amount,
              status: "PENDING",
              methodSnapshot,
              note: "Auto‑Auszahlung (Gewinn)",
            },
          });
        }
      }
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("auto payout (sellLand) failed", _e);
    }

    // Remove earliest certificates to match sold amount
    try {
      const toRemove = requestM2;
      // fetch ids of earliest certificates
      const certs = await db.certificate.findMany({
        where: { userId: user.id },
        orderBy: { purchasedAt: "asc" },
        select: { id: true },
        take: toRemove,
      });
      if (certs.length > 0) {
        await db.certificate.deleteMany({
          where: { id: { in: certs.map((c) => c.id) } },
        });
      }
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("certificate deleteMany (sellLand) failed", _e);
    }

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "SELL",
        m2: requestM2,
        avgPricePerM2: sellCalc.avgPrice,
        feePct: snapshot.sellFeePct,
        totalAmountCHF: net,
        meta: JSON.stringify({
          grossCHF: gross,
          feeCHF: fee,
        }),
      },
    });

    // Increase sell pressure accumulator (price drop at 0.1% net sales)
    await applySellPressure(requestM2);

    const marketRow = await ensureMarket();
    const current = (marketRow as any).currentPrice ?? snapshot.currentPrice;
    const newPrice = toCHF(current * Math.pow(1 - 0.002, requestM2));
    await db.market.update({
      where: { id: marketRow.id },
      data: { pressureUpdatedAt: new Date(), currentPrice: newPrice },
    });

    await _recordMarketPriceSamples({
      startPrice: current,
      amountM2: requestM2,
      kind: "SELL",
    });

    await broadcastMarket();

    return {
      ok: true as const,
      portfolio: await getPortfolio(),
      market: await getMarket(),
      userBalanceCHF: toCHF(updatedUser.balance),
    };
  } catch (error) {
    console.error("sellLand error", error);
    throw error;
  }
}

// Sell specific owned plots selected on the map
export async function sellSelectedPlots(input: {
  coords: Array<{ x: number; y: number }>;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);
    const snapshot = await getPricingSnapshot();

    const coordsUnique = Array.from(
      new Map(
        input.coords.map((c) => [
          c.x + ":" + c.y,
          { x: Math.floor(c.x), y: Math.floor(c.y) },
        ]),
      ).values(),
    );
    if (coordsUnique.length === 0) throw new Error("No cells selected.");

    const plots = await db.plot.findMany({
      where: { OR: coordsUnique.map((c) => ({ x: c.x, y: c.y })) },
    });

    const notOwned = plots.filter((p) => p.ownerId !== user.id);
    if (notOwned.length > 0 || plots.length !== coordsUnique.length) {
      throw new Error("Some selected tiles are not yours.");
    }

    const nCells = coordsUnique.length;
    const scale = await getCellToM2ScaleInternal();
    const n = nCells * scale;
    if (n > user.landOwnedM2 + 1e-6)
      throw new Error("You do not have enough land.");

    const sellCalc = computeDynamicSellProceeds({
      amountM2: n,
      currentPrice: snapshot.currentPrice,
    });
    const gross = sellCalc.gross;
    const fee = toCHF((snapshot.sellFeePct / 100) * gross);
    const net = toCHF(gross - fee);

    // Estimate profit/loss versus last BUY avg price per m²
    const lastBuy = await db.transaction.findFirst({
      where: { userId: user.id, type: "BUY" },
      orderBy: { createdAt: "desc" },
      select: { avgPricePerM2: true },
    });
    const lastAvg = lastBuy?.avgPricePerM2 ?? snapshot.currentPrice;
    const profitCHF = toCHF(net - lastAvg * n);

    const updatedUser = await (async () => {
      const affected = await db.$executeRaw`
        UPDATE User
        SET
          landOwnedM2 = CASE
            WHEN COALESCE(landOwnedM2, 0) >= ${n} THEN COALESCE(landOwnedM2, 0) - ${n}
            ELSE COALESCE(landOwnedM2, 0)
          END,
          balance = ROUND(COALESCE(balance, 0) + ${net}, 2)
        WHERE id = ${user.id}
          AND COALESCE(landOwnedM2, 0) >= ${n}
      `;
      if (!affected) throw new Error("You do not have enough land.");
      const u = await db.user.findUnique({ where: { id: user.id } });
      if (!u) throw new Error("User not found");
      return u;
    })();

    // Remove certificates for these plots (if present)
    try {
      await db.certificate.deleteMany({
        where: { userId: user.id, plotId: { in: plots.map((p) => p.id) } },
      });
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("certificate deleteMany (map sell) failed", _e);
    }

    const housesSold = plots.filter(
      (p) => p.houseType && p.houseType !== "NONE",
    ).length;

    // Areas disabled: no area tracking

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "SELL",
        m2: n,
        avgPricePerM2: sellCalc.avgPrice,
        feePct: snapshot.sellFeePct,
        totalAmountCHF: net,
        meta: JSON.stringify({
          grossCHF: gross,
          feeCHF: fee,
          via: "map_select",
          housesSold,
          lastAvgBuyCHF: lastAvg,
          profitCHF,
        }),
      },
    });

    await applySellPressure(Math.round(n));

    // Clear ownership and houses for the sold plots
    await Promise.all(
      plots.map((p) =>
        db.plot.update({
          where: { id: p.id },
          data: { ownerId: null, houseType: "NONE" },
        }),
      ),
    );

    // Realtime: broadcast changed plots to all clients
    try {
      await setRealtimeStore({
        channelId: "map",
        data: {
          ts: Date.now(),
          changes: plots.map((p) => ({
            x: p.x,
            y: p.y,
            ownerId: null,
            houseType: "NONE",
          })),
        },
      });
    } catch (_e) {
      /* eslint-disable-line @typescript-eslint/no-unused-vars */
      console.error("broadcast map (sellSelectedPlots) failed", _e);
    }

    // Update price and record price sample
    const marketRow = await ensureMarket();
    const current = (marketRow as any).currentPrice ?? snapshot.currentPrice;
    const newPrice = toCHF(current * Math.pow(1 - 0.002, Math.round(n)));
    await db.market.update({
      where: { id: marketRow.id },
      data: { pressureUpdatedAt: new Date(), currentPrice: newPrice },
    });
    await _recordMarketPriceSamples({
      startPrice: current,
      amountM2: Math.round(n),
      kind: "SELL",
    });

    await broadcastMarket();

    return {
      ok: true as const,
      portfolio: await getPortfolio(),
      market: await getMarket(),
      userBalanceCHF: toCHF(updatedUser.balance),
      housesSold,
      profitCHF,
      netCHF: net,
    };
  } catch (error) {
    console.error("sellSelectedPlots error", error);
    throw error;
  }
}

// Build houses (1 house requires 100 m² unassigned)
export async function buildHouses(input: {
  level: HouseLevel;
  count: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);

    const { level, count } = input;

    const lvl = level as HouseLevel;
    if (!["L1", "L2", "L3", "L4", "L5"].includes(lvl)) {
      throw new Error("Invalid house level.");
    }
    const qty = Math.floor(count);
    if (!Number.isFinite(qty) || qty < 1) {
      throw new Error("Invalid house count. Minimum is 1.");
    }

    const houses = await getUserHouses(user.id);
    const assigned = computeAssignedM2(houses);
    const unassigned = user.landOwnedM2 - assigned;
    const required = qty * 100;

    if (required > unassigned) {
      const maxBuildable = Math.floor(unassigned / 100);
      throw new Error(
        `Not enough free m². Available: ${Math.max(0, unassigned)} m² (${maxBuildable} houses).`,
      );
    }

    const newCount = houses[lvl] + qty;
    await db.houseHolding.upsert({
      where: { userId_level: { userId: user.id, level: lvl } },
      update: { count: newCount },
      create: { userId: user.id, level: lvl, count: newCount },
    });

    await db.transaction.create({
      data: {
        userId: user.id,
        type: "BUILD",
        m2: qty * 100,
        avgPricePerM2: 0,
        feePct: 0,
        totalAmountCHF: 0,
        meta: JSON.stringify({ level: lvl, count: qty }),
      },
    });

    return {
      ok: true as const,
      portfolio: await getPortfolio(),
    };
  } catch (error) {
    console.error("buildHouses error", error);
    throw error;
  }
}

// Save or update payout profile for current user
export async function savePayoutProfile(input: {
  method: "PAYPAL" | "IBAN";
  paypalEmail?: string;
  iban?: string;
  accountHolderName?: string;
  country?: string;
  acceptPayoutTerms: boolean;
  autoPayoutOnProfit?: boolean;
}) {
  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    if (!input.acceptPayoutTerms) {
      throw new Error("Please accept the payout terms.");
    }

    if (input.method === "PAYPAL") {
      if (!input.paypalEmail) throw new Error("PayPal email is required.");
    } else if (input.method === "IBAN") {
      if (!input.iban) throw new Error("IBAN is required.");
    }

    const profile = await db.payoutProfile.upsert({
      where: { userId: user.id },
      update: {
        method: input.method,
        paypalEmail:
          input.method === "PAYPAL" ? (input.paypalEmail ?? null) : null,
        iban: input.method === "IBAN" ? (input.iban ?? null) : null,
        accountHolderName: input.accountHolderName ?? null,
        country: input.country ?? null,
        acceptedPayoutTosAt: new Date(),
        autoPayoutOnProfit: input.autoPayoutOnProfit ?? undefined,
      },
      create: {
        userId: user.id,
        method: input.method,
        paypalEmail:
          input.method === "PAYPAL" ? (input.paypalEmail ?? null) : null,
        iban: input.method === "IBAN" ? (input.iban ?? null) : null,
        accountHolderName: input.accountHolderName ?? null,
        country: input.country ?? null,
        acceptedPayoutTosAt: new Date(),
        autoPayoutOnProfit: !!input.autoPayoutOnProfit,
      },
    });

    return {
      ok: true as const,
      profile: {
        method: profile.method,
        paypalEmail: profile.paypalEmail,
        iban: profile.iban,
        accountHolderName: profile.accountHolderName,
        country: profile.country,
        acceptedPayoutTosAt: profile.acceptedPayoutTosAt,
        stripeAccountId: profile.stripeAccountId,
        autoPayoutOnProfit: profile.autoPayoutOnProfit,
      },
    };
  } catch (error) {
    console.error("savePayoutProfile error", error);
    throw error;
  }
}

export async function getPayoutProfile() {
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const user = await ensureUser(auth.userId);
  try {
    const p = await db.payoutProfile.findUnique({ where: { userId: user.id } });
    if (!p) return null;
    return {
      method: p.method as "PAYPAL" | "IBAN",
      paypalEmail: p.paypalEmail,
      iban: p.iban,
      accountHolderName: p.accountHolderName,
      country: p.country,
      acceptedPayoutTosAt: p.acceptedPayoutTosAt,
      isVerified: p.isVerified,
      autoPayoutOnProfit: p.autoPayoutOnProfit,
    };
  } catch (error) {
    // Non-fatal (e.g., transient network/DB error). Treat as no payout method set.
    const msg = (error as any)?.message ?? String(error);
    console.log("getPayoutProfile transient error, returning null:", msg);
    return null;
  }
}

export async function getWalletSummary() {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    await ensureUser(auth.userId);

    const txs = await db.transaction.findMany({
      where: { userId: auth.userId },
      orderBy: { createdAt: "asc" },
      select: {
        type: true,
        m2: true,
        avgPricePerM2: true,
        totalAmountCHF: true,
        meta: true,
      },
    });

    type Lot = { remaining: number; price: number };
    const inventory: Lot[] = [];

    let totalDeposits = 0; // USD
    let totalBuys = 0; // USD spent on buys
    let totalSellsNet = 0; // USD received (net)
    let realizedProfit = 0; // USD
    let realizedCost = 0; // USD cost basis of realized sells
    let realizedRevenue = 0; // USD revenue from realized sells (net)

    for (const tx of txs) {
      const type = (tx.type || "").toUpperCase();
      if (type === "TOPUP") {
        totalDeposits += tx.totalAmountCHF ?? 0;
        continue;
      }

      // Primary market buy
      if (type === "BUY") {
        const units = Math.max(0, tx.m2 || 0);
        const pricePer = Math.max(0, tx.avgPricePerM2 || 0);
        if (units > 0) inventory.push({ remaining: units, price: pricePer });
        totalBuys += tx.totalAmountCHF ?? 0;
        continue;
      }

      // Secondary market buy (1 m² each listing)
      if (type === "SECONDARY_BUY") {
        const units = Math.max(1, tx.m2 || 1);
        // Prefer explicit unit price if provided in meta; fall back to total amount
        let unitPrice = 0;
        try {
          const meta = tx.meta
            ? (JSON.parse(tx.meta) as { priceUSD?: number })
            : undefined;
          unitPrice = Math.max(0, Number(meta?.priceUSD ?? 0));
        } catch {}
        if (!unitPrice)
          unitPrice = Math.max(0, (tx.totalAmountCHF ?? 0) / units);
        inventory.push({ remaining: units, price: unitPrice });
        totalBuys += tx.totalAmountCHF ?? 0;
        continue;
      }

      // Primary market sell (net already includes fees)
      if (type === "SELL") {
        const unitsToSell = Math.max(0, tx.m2 || 0);
        let remaining = unitsToSell;
        let costBasis = 0;
        // FIFO consumption
        while (remaining > 0 && inventory.length > 0) {
          const lot = inventory[0]!;
          const take = Math.min(lot.remaining, remaining);
          costBasis += take * lot.price;
          lot.remaining -= take;
          remaining -= take;
          if (lot.remaining <= 0) inventory.shift();
        }
        const net = tx.totalAmountCHF ?? 0;
        totalSellsNet += net;
        realizedRevenue += net;
        realizedCost += costBasis;
        realizedProfit += net - costBasis;
        continue;
      }

      // Secondary market sold (treat like a sale of m² at given price)
      if (type === "SECONDARY_SOLD") {
        const unitsToSell = Math.max(1, tx.m2 || 1);
        let remaining = unitsToSell;
        let costBasis = 0;
        while (remaining > 0 && inventory.length > 0) {
          const lot = inventory[0]!;
          const take = Math.min(lot.remaining, remaining);
          costBasis += take * lot.price;
          lot.remaining -= take;
          remaining -= take;
          if (lot.remaining <= 0) inventory.shift();
        }
        const net = tx.totalAmountCHF ?? 0; // no platform sell fee here
        totalSellsNet += net;
        realizedRevenue += net;
        realizedCost += costBasis;
        realizedProfit += net - costBasis;
        continue;
      }

      // Ignore other types for P&L
    }

    const realizedPct =
      realizedCost > 0 ? (realizedProfit / realizedCost) * 100 : 0;

    return {
      totalDepositsUSD: toCHF(totalDeposits),
      totalBuysUSD: toCHF(totalBuys),
      totalSellsNetUSD: toCHF(totalSellsNet),
      realizedProfitUSD: toCHF(realizedProfit),
      realizedCostUSD: toCHF(realizedCost),
      realizedRevenueUSD: toCHF(realizedRevenue),
      realizedPct,
    };
  } catch (error) {
    console.error("getWalletSummary error", error);
    throw error;
  }
}

// Profit/Loss summary for a time window
export async function getWalletRangeSummary(input: {
  range: "1D" | "1W" | "MONTH";
}) {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    await ensureUser(auth.userId);

    const now = new Date();
    let since = new Date(now);
    if (input.range === "1D") {
      since = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    } else if (input.range === "1W") {
      since = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    } else {
      // MONTH = from first day of current month
      since = new Date(now.getFullYear(), now.getMonth(), 1);
    }

    const txs = await db.transaction.findMany({
      where: { userId: auth.userId, createdAt: { gte: since } },
      orderBy: { createdAt: "asc" },
      select: { type: true, totalAmountCHF: true },
    });

    const isBuy = (t: string) =>
      ["BUY", "SECONDARY_BUY"].includes((t || "").toUpperCase());
    const isSell = (t: string) =>
      ["SELL", "SECONDARY_SOLD"].includes((t || "").toUpperCase());

    let totalBuys = 0;
    let totalSellsNet = 0;

    for (const tx of txs) {
      const type = (tx.type || "").toUpperCase();
      if (isBuy(type)) totalBuys += tx.totalAmountCHF ?? 0;
      if (isSell(type)) totalSellsNet += tx.totalAmountCHF ?? 0;
    }

    const realized = toCHF(totalSellsNet - totalBuys);
    const realizedPct =
      totalBuys > 0 ? Math.round((realized / totalBuys) * 100 * 100) / 100 : 0;

    return {
      range: input.range,
      totalBuysUSD: toCHF(totalBuys),
      totalSellsNetUSD: toCHF(totalSellsNet),
      realizedUSD: realized,
      realizedPct,
      since,
    };
  } catch (error) {
    console.error("getWalletRangeSummary error", error);
    throw error;
  }
}

// Message Center: recent wallet and land activity for the current user
export async function listMessageCenter(input?: {
  limit?: number;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    const lastSeenAt = (user as any)?.messageCenterLastSeenAt
      ? new Date((user as any).messageCenterLastSeenAt)
      : null;

    const limit = Math.min(100, Math.max(1, Math.floor(input?.limit ?? 25)));
    const txs = await db.transaction.findMany({
      where: { userId: user.id },
      orderBy: { createdAt: "desc" },
      take: limit,
      select: {
        id: true,
        type: true,
        m2: true,
        avgPricePerM2: true,
        feePct: true,
        totalAmountCHF: true,
        meta: true,
        createdAt: true,
      },
    });

    const items = txs
      .map((t) => {
        const type = (t.type || "").toUpperCase();
        let amountUSD = toCHF(t.totalAmountCHF ?? 0);
        const m2 = Math.max(0, t.m2 || 0);
        const isNew = lastSeenAt ? t.createdAt > lastSeenAt : true;
        let title = "Wallet activity";
        let detail = "";
        let info = "";
        let direction: "in" | "out" | "neutral" = "neutral";
        let skip = false;

        if (type === "TOPUP") {
          direction = "in";
          title = "Wallet top-up (credit added)";
          let productName = "Wallet top-up";
          let occurrence: number | null = null;
          try {
            const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
            productName = meta?.productName
              ? String(meta.productName)
              : productName;
            occurrence =
              typeof meta?.occurrence === "number"
                ? Number(meta.occurrence)
                : null;
          } catch {}
          // Show the exact credited amount (always 2 decimals).
          detail = `+${toCHF(amountUSD).toFixed(2)} USD • ${productName}${occurrence ? ` (#${occurrence})` : ""}`;
          info = `Action: Wallet top-up (credit added)\nMarket: Not applicable\nMoney: +${toCHF(amountUSD).toFixed(2)} USD credited to your balance\nWhat changed: Your wallet balance increased (no land m² changed)\nSource: PP coin top-up`;
        } else if (type === "BUY") {
          direction = "out";
          title = `Primary market m² purchase — ${m2} m²`;
          const avg = t.avgPricePerM2 ? toCHF(t.avgPricePerM2) : null;
          detail = `${m2} m² • Total: ${toCHF(amountUSD)}${avg != null ? ` • Avg: ${avg}/m²` : ""}`;
          info = `Action: Primary market m² purchase\nMarket: Primary\nm²: ${m2} m²\nMoney: −${toCHF(amountUSD)} debited from your balance\nPricing: ${avg != null ? `Avg ${avg} per m²` : "Primary market price"}\nWhat changed: Your wallet balance decreased and your owned m² increased`;
        } else if (type === "SELL") {
          direction = "in";
          title = `Primary market m² sale — ${m2} m² (net)`;
          try {
            const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
            if (
              typeof meta?.feeCHF === "number" &&
              typeof meta?.grossCHF === "number"
            ) {
              const gross = toCHF(meta.grossCHF);
              const fee = toCHF(meta.feeCHF);
              const net = toCHF(meta.grossCHF - meta.feeCHF);
              if (Number.isFinite(net)) amountUSD = net;
              detail = `${m2} m² • Net: ${toCHF(net)} • Fee: ${toCHF(fee)} • Gross: ${toCHF(gross)}`;
              info = `Action: Primary market m² sale (net)\nMarket: Primary\nm²: ${m2} m²\nGross: ${toCHF(gross)}\nFee: ${toCHF(fee)}\nNet: +${toCHF(net)} credited to your balance\nWhat changed: Your wallet balance increased and your owned m² decreased`;
            } else {
              detail = `${m2} m² • +${toCHF(amountUSD)} (credited)`;
              info = `Action: Primary market m² sale\nMarket: Primary\nm²: ${m2} m²\nMoney: +${toCHF(amountUSD)} credited to your balance\nWhat changed: Your wallet balance increased and your owned m² decreased`;
            }
          } catch {
            detail = `${m2} m² • +${toCHF(amountUSD)} (credited)`;
            info = `Action: Primary market m² sale\nMarket: Primary\nm²: ${m2} m²\nMoney: +${toCHF(amountUSD)} credited to your balance\nWhat changed: Your wallet balance increased and your owned m² decreased`;
          }
        } else if (type === "SECONDARY_BUY") {
          direction = "out";
          title = "2nd market m² purchase — 1 m²";
          let price = amountUSD;
          try {
            const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
            if (typeof meta?.priceUSD === "number")
              price = toCHF(meta.priceUSD);
          } catch {}
          if (Number.isFinite(price)) amountUSD = price;
          detail = `1 m² • Total: ${toCHF(amountUSD)} (paid)`;
          info = `Action: 2nd market m² purchase\nMarket: 2nd Market\nm²: 1 m²\nMoney: −${toCHF(amountUSD)} debited from your balance\nWhat changed: Your wallet balance decreased and you received 1 m² from another user\nNotes: Payment is processed and ownership is transferred after completion`;
        } else if (type === "SECONDARY_SOLD") {
          direction = "in";
          title = "2nd market m² sale — 1 m² (net)";
          try {
            const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
            const gross =
              typeof meta?.priceUSD === "number"
                ? toCHF(meta.priceUSD)
                : amountUSD;
            const fee =
              typeof meta?.commissionUSD === "number"
                ? toCHF(meta.commissionUSD)
                : 0;
            const net =
              typeof meta?.netSaleUSD === "number"
                ? toCHF(meta.netSaleUSD)
                : toCHF(gross - Math.max(0, fee));
            const profitNet =
              typeof meta?.netProfitUSD === "number"
                ? toCHF(meta.netProfitUSD)
                : null;
            const profitGross =
              typeof meta?.profitGrossUSD === "number"
                ? toCHF(meta.profitGrossUSD)
                : null;

            // Show net as the message amount.
            if (Number.isFinite(net)) amountUSD = net;

            const parts: string[] = [
              `Net: ${toCHF(net)}`,
              `Fee: ${toCHF(fee)}`,
              `Gross: ${toCHF(gross)}`,
            ];
            if (profitNet != null)
              parts.push(`Profit: ${toCHF(profitNet)} (net)`);
            else if (profitGross != null)
              parts.push(`Profit: ${toCHF(profitGross)}`);
            detail = parts.join(" • ");

            info = `Action: 2nd market m² sale (net)\nMarket: 2nd Market\nm²: 1 m²\nGross: ${toCHF(gross)}\nFee: ${toCHF(fee)}\nNet: +${toCHF(net)} credited to your balance\n${profitNet != null ? `Profit (net): ${toCHF(profitNet)}` : profitGross != null ? `Profit: ${toCHF(profitGross)}` : "Profit: not available"}\nWhat changed: Your wallet balance increased and you transferred 1 m² to another user`;
          } catch {
            detail = `1 m² • +${toCHF(amountUSD)} (credited)`;
            info = `Action: 2nd market m² sale\nMarket: 2nd Market\nm²: 1 m²\nMoney: +${toCHF(amountUSD)} credited to your balance\nWhat changed: Your wallet balance increased and you transferred 1 m² to another user`;
          }
        } else if (type === "COMMISSION_IN") {
          direction = "in";
          title = "2nd market fee received";
          detail = `+${toCHF(amountUSD)} credited`;
          info = `Action: 2nd market fee received\nMarket: 2nd Market\nMoney: +${toCHF(amountUSD)} credited to your balance\nWhat changed: Your wallet balance increased\nNotes: This is a fee/commission paid by another user (platform rules)`;
        } else if (type === "COMMISSION_OUT") {
          // If this commission is tied to a 2nd-market sale, we already show the net amount in the sale message.
          // Hide this separate debit to avoid confusing duplicates.
          try {
            const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
            if (String(meta?.on ?? "").toUpperCase() === "SECONDARY_SOLD") {
              skip = true;
            }
          } catch {}

          if (!skip) {
            direction = "out";
            title = "2nd market fee paid";
            detail = `−${toCHF(amountUSD)} debited`;
            info = `Action: 2nd market fee paid\nMarket: 2nd Market\nMoney: −${toCHF(amountUSD)} debited from your balance\nWhat changed: Your wallet balance decreased\nNotes: This is a fee/commission charged by platform rules`;
          }
        }

        if (skip) return null;

        return {
          id: t.id,
          createdAt: t.createdAt,
          kind: type,
          title,
          detail,
          info,
          amountUSD,
          direction,
          isNew,
        };
      })
      .filter((x): x is any => !!x);

    // Only show actual money movements with non-zero amounts and clear in/out direction
    return items.filter(
      (it) =>
        (it.direction === "in" || it.direction === "out") &&
        Math.abs(it.amountUSD ?? 0) > 0.0,
    );
  } catch (error) {
    console.error("listMessageCenter error", error);
    throw error;
  }
}

export async function getMessageCenterUnreadCount(input?: {
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const user = await ensureUserWithCompliance(userId);

    const lastSeenAt = (user as any)?.messageCenterLastSeenAt
      ? new Date((user as any).messageCenterLastSeenAt)
      : null;

    const kinds = [
      "TOPUP",
      "BUY",
      "SELL",
      "SECONDARY_BUY",
      "SECONDARY_SOLD",
      "COMMISSION_IN",
      "COMMISSION_OUT",
    ];

    const where: any = {
      userId,
      type: { in: kinds },
      totalAmountCHF: { not: 0 },
    };
    if (lastSeenAt) {
      where.createdAt = { gt: lastSeenAt };
    }

    // We can't do JSON filtering in SQLite/Prisma reliably here, so we compute the count in code
    // to match the Message Center display rules (e.g., hiding commission debits for 2nd-market sales).
    const txs = await db.transaction.findMany({
      where,
      orderBy: { createdAt: "desc" },
      take: 500,
      select: {
        id: true,
        type: true,
        m2: true,
        avgPricePerM2: true,
        feePct: true,
        totalAmountCHF: true,
        meta: true,
        createdAt: true,
      },
    });

    let unread = 0;
    for (const t of txs) {
      const type = (t.type || "").toUpperCase();
      const amountUSD = toCHF(t.totalAmountCHF ?? 0);
      let direction: "in" | "out" | "neutral" = "neutral";
      let skip = false;

      if (type === "TOPUP") direction = "in";
      else if (type === "BUY") direction = "out";
      else if (type === "SELL") direction = "in";
      else if (type === "SECONDARY_BUY") direction = "out";
      else if (type === "SECONDARY_SOLD") direction = "in";
      else if (type === "COMMISSION_IN") direction = "in";
      else if (type === "COMMISSION_OUT") {
        direction = "out";
        try {
          const meta = t.meta ? (JSON.parse(t.meta) as any) : null;
          if (String(meta?.on ?? "").toUpperCase() === "SECONDARY_SOLD") {
            skip = true;
          }
        } catch {}
      }

      if (skip) continue;
      if (!(direction === "in" || direction === "out")) continue;
      if (Math.abs(amountUSD ?? 0) <= 0.0) continue;
      unread++;
    }

    return { unreadCount: Math.max(0, unread) };
  } catch (error) {
    console.error("getMessageCenterUnreadCount error", error);
    throw error;
  }
}

export async function markMessageCenterSeen(input?: { accessToken?: string }) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    await ensureUserWithCompliance(userId);
    const seenAt = new Date();

    await db.user.update({
      where: { id: userId },
      data: { messageCenterLastSeenAt: seenAt },
    });

    return { ok: true, seenAt };
  } catch (error) {
    console.error("markMessageCenterSeen error", error);
    throw error;
  }
}

// Full transaction history for current user (for receipts)
export async function listMyTransactions() {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    await ensureUser(auth.userId);
    const txs = await db.transaction.findMany({
      where: { userId: auth.userId },
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        type: true,
        m2: true,
        avgPricePerM2: true,
        feePct: true,
        totalAmountCHF: true,
        meta: true,
        createdAt: true,
      },
    });
    return txs.map((t) => ({
      id: t.id,
      type: t.type,
      m2: t.m2,
      avgPricePerM2: t.avgPricePerM2,
      feePct: t.feePct,
      totalAmountUSD: toCHF(t.totalAmountCHF),
      meta: t.meta ?? null,
      createdAt: t.createdAt,
    }));
  } catch (error) {
    console.error("listMyTransactions error", error);
    throw error;
  }
}

// Simple profile getters/setters for local overrides
export async function getProfile(input?: { accessToken?: string }) {
  try {
    const auth = await getAuth();
    const cfg = await db.appConfig.findUnique({
      where: { key: "DEFAULT" },
      select: { reportInboxEmail: true },
    });

    const token = (input?.accessToken || "").trim();
    let ownerByEmail = false;

    const userId = token
      ? await (async () => {
          try {
            const supabase = requireSupabaseClient();
            const res = await supabase.auth.getUser(token);
            const u = (res as any)?.data?.user;
            const id = u?.id ? String(u.id) : "";
            const email = u?.email ? String(u.email) : "";
            ownerByEmail =
              !!email && email.toLowerCase() === OWNER_EMAIL.toLowerCase();
            if (!id) throw new Error("missing");
            return id;
          } catch {
            throw new Error("missing");
          }
        })()
      : auth.status === "authenticated" && auth.userId
        ? auth.userId
        : null;

    if (!userId) {
      return {
        displayName: null,
        profileImageUrl: null,
        language: null,
        themePreset: null,
        currency: "EUR",
        balance: 0,
        landOwnedM2: 0,
        mainMarketPurchasedM2: 0,
        secondaryMarketPurchasedM2: 0,
        isAdmin: false,
        isOwner: false,
        reportInboxEmail: cfg?.reportInboxEmail || "",
        emailNotificationsEnabled: false,
        emailNotificationsEveryMin: 60,
        emailNotificationsQuietStart: null,
        emailNotificationsQuietEnd: null,
        notificationTopics: [],
      };
    }

    const user = await ensureUser(userId);
    const isOwner = ownerByEmail || user.id === OWNER_USER_ID;

    // Keep admin flag in sync for the owner (even if they log in via Supabase)
    if (isOwner && !user.isAdmin) {
      try {
        await db.user.update({
          where: { id: user.id },
          data: { isAdmin: true },
        });
      } catch {}
    }

    return {
      displayName: user.displayNameLocal ?? null,
      profileImageUrl: user.profileImageUrl ?? null,
      language: user.language ?? null,
      themePreset: (user as any).themePreset ?? null,
      currency: (user as any).currency ?? "USD",
      balance: user.balance,
      landOwnedM2: user.landOwnedM2,
      mainMarketPurchasedM2: Number((user as any).mainMarketPurchasedM2 ?? 0),
      secondaryMarketPurchasedM2: Number(
        (user as any).secondaryMarketPurchasedM2 ?? 0,
      ),
      isAdmin: !!user.isAdmin,
      isOwner,
      reportInboxEmail: cfg?.reportInboxEmail || "",
      // E-Mail-Benachrichtigungen sind komplett deaktiviert.
      emailNotificationsEnabled: false,
      emailNotificationsEveryMin: 60,
      emailNotificationsQuietStart: null,
      emailNotificationsQuietEnd: null,
      notificationTopics: [],
    };
  } catch (error) {
    console.error("getProfile error", error);
    // Return safe defaults to prevent unauth errors surfacing to the UI
    return {
      displayName: null,
      profileImageUrl: null,
      language: null,
      themePreset: null,
      currency: "EUR",
      balance: 0,
      landOwnedM2: 0,
      mainMarketPurchasedM2: 0,
      secondaryMarketPurchasedM2: 0,
      isAdmin: false,
      isOwner: false,
      reportInboxEmail: "",
      emailNotificationsEnabled: false,
      emailNotificationsEveryMin: 60,
      emailNotificationsQuietStart: null,
      emailNotificationsQuietEnd: null,
      notificationTopics: [],
    };
  }
}

export async function updateEmailNotificationSettings(_input: {
  enabled?: boolean;
  everyMin?: number;
  quietStart?: string | null;
  quietEnd?: string | null;
  topics?: string[];
}) {
  void _input;
  // Email notifications are fully disabled.
  // We return a stable response so the app keeps working even if the settings UI still calls this.
  return {
    ok: true as const,
    emailNotificationsEnabled: false,
    emailNotificationsEveryMin: 60,
    emailNotificationsQuietStart: null as string | null,
    emailNotificationsQuietEnd: null as string | null,
    notificationTopics: _getUserNotificationTopics(),
    needsEmailPermission: false,
    disabled: true as const,
  };
}

export async function sendTestEmailNotification() {
  // Email sending is disabled.
  return { ok: true as const, needsEmailPermission: false, disabled: true };
}

export async function updateProfile(input: {
  accessToken?: string;
  displayName?: string;
  profileImageBase64?: string;
  language?: "de" | "en" | "es";
  themePreset?: "blue" | "teal" | "violet";
  currency?: string; // ISO 4217 currency code
}) {
  try {
    const auth = await getAuth({ required: false });
    const userId = input?.accessToken
      ? await requireSupabaseUserId(input.accessToken)
      : auth.status === "authenticated" && auth.userId
        ? auth.userId
        : null;

    if (!userId) {
      throw new Error("Please sign in to continue.");
    }

    const user = await ensureUser(userId);

    // Enforce unique username (case-insensitive) across users when provided
    const desiredName = input.displayName?.trim();
    if (desiredName) {
      const collisions = await db.user.findMany({
        where: { displayNameLocal: { not: null } },
        select: { id: true, displayNameLocal: true },
      });
      const taken = collisions.some(
        (u) =>
          u.id !== user.id &&
          (u.displayNameLocal ?? "").toLowerCase() ===
            (desiredName ?? "").toLowerCase(),
      );
      if (taken) {
        throw new Error("That username is already taken.");
      }
    }

    let uploadedUrl: string | undefined;
    if (input.profileImageBase64) {
      try {
        uploadedUrl = await upload({
          bufferOrBase64: input.profileImageBase64,
          fileName: `profile-${user.id}.png`,
        });
      } catch (_e) {
        /* eslint-disable-line @typescript-eslint/no-unused-vars */
        console.error("updateProfile upload failed", _e);
      }
    }

    const updated = await db.user.update({
      where: { id: user.id },
      data: {
        displayNameLocal: desiredName ?? undefined,
        profileImageUrl: uploadedUrl ?? undefined,
        language: input.language ?? undefined,
        themePreset: input.themePreset ?? undefined,
        currency: (input.currency || undefined) as any,
      },
      select: {
        id: true,
        displayNameLocal: true,
        profileImageUrl: true,
        language: true,
        themePreset: true,
        currency: true,
      },
    });

    return {
      ok: true as const,
      displayName: updated.displayNameLocal ?? null,
      profileImageUrl: updated.profileImageUrl ?? null,
      themePreset: updated.themePreset ?? null,
      currency: (updated as any).currency ?? "USD",
    };
  } catch (error) {
    console.error("updateProfile error", error);
    throw error;
  }
}

export async function setReportInboxEmail(input: { email: string }) {
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const me = await ensureUser(auth.userId);
  if (!me.isAdmin) throw new Error("Admin only");

  const email = (input.email || "").trim();
  if (email.length > 0 && (!email.includes("@") || email.length < 5)) {
    throw new Error("Please enter a valid email address");
  }

  await db.appConfig.upsert({
    where: { key: "DEFAULT" },
    create: { key: "DEFAULT", reportInboxEmail: email },
    update: { reportInboxEmail: email },
  });

  return { ok: true, reportInboxEmail: email };
}

export async function submitReport(input: {
  area: string;
  title: string;
  details: string;
  commonIssues?: string[];
  anonymous?: boolean;
  pagePath?: string;
  pageUrl?: string;
  createdAt?: string;
}) {
  const auth = await getAuth({ required: false });

  const cfg = await db.appConfig.findUnique({ where: { key: "DEFAULT" } });
  const inboxEmail = (cfg?.reportInboxEmail || "").trim();
  if (inboxEmail.length < 5 || !inboxEmail.includes("@")) {
    throw new Error(
      "Report inbox email is not set. Ask an admin to set it at the bottom of the report form.",
    );
  }

  const title = (input.title || "").trim();
  const details = (input.details || "").trim();
  const area = (input.area || "").trim();
  if (title.length < 3) throw new Error("Please enter a title");
  if (details.length < 5) throw new Error("Please enter details");

  const commonIssues = Array.isArray(input.commonIssues)
    ? input.commonIssues.filter((x) => typeof x === "string" && x.trim().length)
    : [];

  const labelByKey: Record<string, string> = {
    SLOW: "App is slow / freezing",
    LOGIN: "Sign-in / session issues",
    BUY_SELL: "Can't buy or sell",
    BALANCE: "Wallet balance looks wrong",
    MAP: "Map not loading",
    IMAGES: "Images not loading",
  };

  const commonIssueLabels = commonIssues.map((k) => labelByKey[k] || k);

  const createdAt = (() => {
    const raw = (input.createdAt || "").trim();
    if (!raw) return new Date().toISOString();
    const d = new Date(raw);
    if (Number.isNaN(d.getTime())) return new Date().toISOString();
    return d.toISOString();
  })();

  let reporterLine = "Anonymous";
  const anonymous = !!input.anonymous;
  if (auth.status === "authenticated" && !anonymous) {
    const me = await ensureUser(auth.userId);
    const handle = (me as any)?.handle ? `@${(me as any).handle}` : "";
    const name = (me as any)?.name ? `${(me as any).name}` : "";
    reporterLine = [name, handle].filter(Boolean).join(" ").trim();
    if (!reporterLine) reporterLine = auth.userId;
  }

  const baseUrl = getBaseUrl();
  const openLink = baseUrl;

  const lines: string[] = [];
  lines.push("### New report");
  lines.push("");
  lines.push(`- **Area:** ${area || "(not set)"}`);
  if ((input.pagePath || "").trim().length)
    lines.push(`- **Path:** ${(input.pagePath || "").trim()}`);
  if ((input.pageUrl || "").trim().length)
    lines.push(`- **URL:** ${(input.pageUrl || "").trim()}`);
  if (commonIssueLabels.length)
    lines.push(`- **Common issues:** ${commonIssueLabels.join(", ")}`);
  lines.push(`- **Anonymous:** ${anonymous ? "Yes" : "No"}`);
  lines.push(`- **Reporter:** ${reporterLine}`);
  lines.push(`- **Submitted:** ${createdAt}`);
  lines.push("");
  lines.push("---");
  lines.push("");
  lines.push(`**Title:** ${title}`);
  lines.push("");
  lines.push("**Details:**");
  lines.push("");
  lines.push(details);
  lines.push("");
  lines.push(`Open the app: ${openLink}`);

  await inviteUser({
    email: inboxEmail,
    subject: `Report: ${title}`,
    markdown: lines.join("\n"),
    unauthenticatedLinks: true,
  });

  return { ok: true };
}

export async function submitWithdrawalRequest(input: {
  amountCHF: number;
  note?: string;
}) {
  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    const amount = Math.round((input.amountCHF ?? 0) * 100) / 100;
    if (!Number.isFinite(amount) || amount <= 0) {
      throw new Error("Invalid amount.");
    }
    if (amount > user.balance) {
      throw new Error("Amount exceeds available balance.");
    }

    const profile = await db.payoutProfile.findUnique({
      where: { userId: user.id },
    });
    if (!profile || !profile.acceptedPayoutTosAt) {
      throw new Error("Please set up a payout profile first.");
    }

    const methodSnapshot = JSON.stringify({
      method: profile.method,
      paypalEmail: profile.paypalEmail,
      iban: profile.iban,
      accountHolderName: profile.accountHolderName,
      country: profile.country,
    });

    // Deduct balance and create request
    await db.user.update({
      where: { id: user.id },
      data: { balance: toCHF(user.balance - amount) },
    });

    const req = await db.withdrawalRequest.create({
      data: {
        userId: user.id,
        amountCHF: amount,
        status: "PENDING",
        methodSnapshot,
        note: input.note ?? null,
      },
    });

    return { ok: true as const, requestId: req.id };
  } catch (error) {
    console.error("submitWithdrawalRequest error", error);
    throw error;
  }
}

// Leaderboard: top users by estimated land value (including house bonuses)
export async function getLeaderboard() {
  try {
    const snapshot = await getPricingSnapshot();
    const plots = await db.plot.findMany({
      where: {},
      select: { ownerId: true },
    });
    const landByUser: Record<string, number> = {};
    for (const p of plots) {
      if (!p.ownerId) continue;
      landByUser[p.ownerId] = (landByUser[p.ownerId] ?? 0) + 1;
    }
    const userIds = Object.keys(landByUser);
    if (!userIds.length) {
      return {
        pricePerM2CHF: toCHF(snapshot.currentPrice),
        leaderboard: [] as any[],
      };
    }
    const users = await db.user.findMany({
      select: { id: true, name: true, image: true },
      where: { id: { in: userIds } },
    });

    // Load houses for all users
    const housesAll = await db.houseHolding.findMany({
      where: { userId: { in: users.map((u) => u.id) } },
    });

    const byUserHouses: Record<string, Record<HouseLevel, number>> = {};
    for (const user of users) {
      byUserHouses[user.id] = { L1: 0, L2: 0, L3: 0, L4: 0, L5: 0 };
    }
    for (const h of housesAll) {
      const lvl = h.level as HouseLevel;
      const container =
        byUserHouses[h.userId] ??
        (byUserHouses[h.userId] = { L1: 0, L2: 0, L3: 0, L4: 0, L5: 0 });
      if (
        lvl === "L1" ||
        lvl === "L2" ||
        lvl === "L3" ||
        lvl === "L4" ||
        lvl === "L5"
      ) {
        container[lvl] = h.count;
      }
    }

    const rows = users
      .map((u) => {
        const houses = byUserHouses[u.id] ?? {
          L1: 0,
          L2: 0,
          L3: 0,
          L4: 0,
          L5: 0,
        };
        const userLand = landByUser[u.id] ?? 0;
        const value = computeEstimatedValueCHF({
          landOwnedM2: userLand,
          houses,
          currentPrice: snapshot.currentPrice,
        });
        return {
          userId: u.id,
          name: u.name ?? `User ${u.id.slice(0, 6)}`,
          image: u.image ?? null,
          landOwnedM2: landByUser[u.id] ?? 0,
          estimatedValueCHF: value,
          houses,
        };
      })
      .sort((a, b) => b.estimatedValueCHF - a.estimatedValueCHF)
      .slice(0, 100);

    return {
      pricePerM2CHF: toCHF(snapshot.currentPrice),
      leaderboard: rows,
    };
  } catch (error) {
    console.error("getLeaderboard error", error);
    throw error;
  }
}

export async function setCertificateTemplateUrl(input: { url: string }) {
  try {
    {
      const auth = await getAuth({ required: false });
      if (auth.status !== "authenticated" || !auth.userId) {
        throw new Error("Please sign in to continue.");
      }
    }
    const market = await ensureMarket();
    const updated = await db.market.update({
      where: { id: market.id },
      data: { certificateTemplateUrl: input.url },
      select: { certificateTemplateUrl: true },
    });
    return {
      ok: true as const,
      certificateTemplateUrl: updated.certificateTemplateUrl,
    };
  } catch (error) {
    console.error("setCertificateTemplateUrl error", error);
    throw error;
  }
}

export async function setCertificateTemplateFromBase64(input: {
  base64: string;
  fileName?: string;
}) {
  try {
    {
      const auth = await getAuth({ required: false });
      if (auth.status !== "authenticated" || !auth.userId) {
        throw new Error("Please sign in to continue.");
      }
    }
    const url = await upload({
      bufferOrBase64: input.base64,
      fileName: input.fileName ?? `certificate-template-${Date.now()}.png`,
    });
    const market = await ensureMarket();
    const updated = await db.market.update({
      where: { id: market.id },
      data: { certificateTemplateUrl: url },
      select: { certificateTemplateUrl: true },
    });
    return {
      ok: true as const,
      certificateTemplateUrl: updated.certificateTemplateUrl,
    };
  } catch (error) {
    console.error("setCertificateTemplateFromBase64 error", error);
    throw error;
  }
}

// Pricing rules text (for modal)
export async function getPricingRules() {
  const snapshot = await getPricingSnapshot();
  return {
    basePrice: snapshot.basePrice,
    stepSizeM2: 0,
    stepIncreasePct: 0,
    sellFeePct: snapshot.sellFeePct,
    houseBonuses: HOUSE_BONUS,
    notes:
      "Preis bewegt sich dauerhaft mit Angebots- und Nachfrage-Druck: pro gekauftem m² steigt der Preis um ca. $ " +
      snapshot.kPerM2 +
      ", pro verkauftem m² fällt er um denselben Betrag. Häuser: Basic=8 m², Villa=19 m², Mansion=36 m² (max 36 m² pro Bauvorgang).",
  };
}

// List certificates for current user
export async function listCertificates(input?: { accessToken?: string }) {
  try {
    const userId = await requireSupabaseUserId(input?.accessToken);
    await ensureUser(userId);
    const certs = await db.certificate.findMany({
      where: { userId },
      orderBy: { purchasedAt: "desc" },
      select: {
        id: true,
        plotId: true,
        purchasedAt: true,
        plot: { select: { x: true, y: true } },
      },
    });
    return certs.map((c) => ({
      id: c.id,
      purchasedAt: c.purchasedAt,
      plot: c.plot ? { x: c.plot.x, y: c.plot.y } : null,
    }));
  } catch (error) {
    console.error("listCertificates error", error);
    throw error;
  }
}

export async function listGuestCertificates(input: { deviceId: string }) {
  try {
    const userId = requireGuestDeviceId(input?.deviceId);
    await ensureUser(userId);
    const certs = await db.certificate.findMany({
      where: { userId },
      orderBy: { purchasedAt: "desc" },
      select: {
        id: true,
        plotId: true,
        purchasedAt: true,
        plot: { select: { x: true, y: true } },
      },
    });
    return certs.map((c) => ({
      id: c.id,
      purchasedAt: c.purchasedAt,
      plot: c.plot ? { x: c.plot.x, y: c.plot.y } : null,
    }));
  } catch (error) {
    console.error("listGuestCertificates error", error);
    throw error;
  }
}

// Islands API
// Note: We write Island rows via a transaction in createIsland().
// This small unreachable block exists to make it explicit that Island data is user-created.
async function _islandModelWriteMarker() {
  if (false) {
    await db.island.create({
      data: {
        ownerId: "0",
        name: "marker",
        iconUrl: null,
        ticker: null,
        totalSupplyM2: 1,
        startPriceUSD: 0.01,
        stepUpPct: 0,
        stepDownPct: 0,
        commissionPct: 5,
        ownerSharePct: 80,
        platformSharePct: 20,
        soldM2: 0,
      },
    });
  }
}
void _islandModelWriteMarker;

export async function suggestIslandStepPct(input: { totalSupplyM2: number }) {
  try {
    const s = Math.max(1, Math.floor(input?.totalSupplyM2 ?? 0));
    const val = Math.sqrt((5 * 10000) / s);
    const suggested = Math.max(0.1, Math.min(5, Math.round(val * 100) / 100));
    return { suggestedPct: suggested };
  } catch (error) {
    console.error("suggestIslandStepPct error", error);
    throw error;
  }
}

export async function createIsland(input: {
  name: string;
  description?: string;
  ticker?: string;
  totalSupplyM2: number;
  startPriceUSD: number;
  stepUpPct: number; // %‑Anstieg bei Kauf
  stepDownPct: number; // %‑Abfall bei Verkauf
  iconBase64?: string;
  ownerInitialM2?: number; // direkt dem Ersteller gehörende m²
  diagId?: string; // optional: Diagnose-ID vom Client
  accessToken?: string;
}) {
  const normalizeCreateIslandError = (err: unknown) => {
    const msg = err instanceof Error ? err.message : String(err ?? "");
    const lower = msg.toLowerCase();
    const code = (err as any)?.code ? String((err as any).code) : "";

    if (
      lower.includes("missingautherror") ||
      lower.includes("unauthenticated")
    ) {
      return "Please sign in to create an island.";
    }

    if (
      lower.includes("sqlite_busy") ||
      lower.includes("database is locked") ||
      lower.includes("busy") ||
      lower.includes("timeout") ||
      lower.includes("timed out")
    ) {
      return "The server is busy right now. Please try again in a few seconds.";
    }

    if (code === "P2002" || lower.includes("unique constraint failed")) {
      return "That name or ticker is already taken. Please choose another one.";
    }

    if (lower.includes("foreign key") || code === "P2003") {
      return "Could not link the island right now. Please try again.";
    }

    if (lower.includes("not found")) {
      return "User not found. Please reload the page and try again.";
    }

    if (msg && msg.trim().length > 0) return msg.trim();
    return "Could not create the island right now.";
  };

  const safeTrim = (s: unknown, max = 600) => {
    const str = String(s ?? "").trim();
    return str.length > max ? str.slice(0, max) : str;
  };

  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    const diagId = String((input as any)?.diagId ?? "").trim();

    const name = String(input?.name ?? "").trim();
    const descriptionRaw = String((input as any)?.description ?? "").trim();
    const description =
      descriptionRaw.length > 0 ? safeTrim(descriptionRaw, 1200) : null;
    const ticker =
      String(input?.ticker ?? "")
        .trim()
        .toUpperCase() || null;
    const totalSupplyM2 = Math.max(1, Math.floor(input?.totalSupplyM2 ?? 0));
    const startPriceUSD = Math.max(0.0001, Number(input?.startPriceUSD ?? 0));
    const stepUpPct = Number(input?.stepUpPct ?? 0);
    const stepDownPct = Number(input?.stepDownPct ?? 0);
    const ownerInitialM2Raw = Math.max(
      0,
      Math.floor(Number(input?.ownerInitialM2 ?? 0)),
    );
    const ownerInitialM2 = Math.min(totalSupplyM2, ownerInitialM2Raw);
    if (!name) throw new Error("Name is required.");
    if (!Number.isFinite(totalSupplyM2) || totalSupplyM2 <= 0)
      throw new Error("Invalid m² amount.");
    if (totalSupplyM2 > 4000) {
      throw new Error("Maximum 4,000 m² per island.");
    }
    if (!Number.isFinite(startPriceUSD) || startPriceUSD <= 0)
      throw new Error("Invalid starting price.");
    if (!Number.isFinite(stepUpPct) || !Number.isFinite(stepDownPct)) {
      throw new Error("Invalid price change.");
    }
    if (stepUpPct < 0 || stepUpPct > 5 || stepDownPct < 0 || stepDownPct > 5) {
      throw new Error("Maximum 5% price change per buy/sell.");
    }

    const START_FEE_USD = 5;
    const me = await db.user.findUnique({ where: { id: user.id } });
    if (!me) {
      throw new Error("User not found.");
    }

    // Rule: You can only create an island after buying at least 2 m² directly
    // from the Main Market (not from the 2nd Market).
    const mainBought = Math.max(
      0,
      Number((me as any).mainMarketPurchasedM2 ?? 0),
    );
    if (mainBought < 2) {
      throw new Error(
        "You need at least 2 m² bought from the Main Market (not the 2nd Market) to create an island.",
      );
    }

    const initialCost = ownerInitialM2 > 0 ? startPriceUSD * ownerInitialM2 : 0;
    const totalCost = START_FEE_USD + initialCost;
    if (me.balance < totalCost) {
      throw new Error(
        "Insufficient balance for start fee and owned m² (start price * owned m²).",
      );
    }

    if (diagId) {
      const diagInput = {
        name,
        ticker,
        totalSupplyM2,
        startPriceUSD,
        stepUpPct,
        stepDownPct,
        ownerInitialM2,
        startFeeUSD: START_FEE_USD,
        totalCost,
        hasIcon: !!input?.iconBase64,
      };
      try {
        await db.createIslandDiag.upsert({
          where: { diagId },
          create: {
            diagId,
            userId: user.id,
            status: "STARTED",
            ok: false,
            inputJson: safeTrim(JSON.stringify(diagInput), 4000),
          },
          update: {
            userId: user.id,
            status: "STARTED",
            ok: false,
            inputJson: safeTrim(JSON.stringify(diagInput), 4000),
            errorMessage: null,
            errorName: null,
            errorCode: null,
            islandId: null,
          },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "unknown";
        console.log("[createIsland] diag upsert failed:", msg);
      }
    }

    let iconUrl: string | null = null;
    if (input?.iconBase64) {
      try {
        iconUrl = await upload({
          bufferOrBase64: input.iconBase64,
          fileName: `island-icon-${Date.now()}.png`,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "unknown";
        console.log(
          "[createIsland] icon upload failed:",
          msg.slice(0, 180),
          "len=",
          String(input.iconBase64).length,
        );
      }
      if (!iconUrl) throw new Error("Icon upload failed.");
    }

    if (!Number.isFinite(me.balance)) {
      throw new Error("Invalid balance.");
    }
    if (!Number.isFinite(totalCost) || totalCost < 0) {
      throw new Error("Invalid cost calculation.");
    }

    console.log(
      "[createIsland] start:",
      user.id,
      name,
      "supply",
      totalSupplyM2,
      "price",
      startPriceUSD,
      "step",
      stepUpPct,
      "own",
      ownerInitialM2,
      "icon",
      !!iconUrl,
      diagId ? `diag=${diagId}` : "",
    );

    const isTransientDbError = (e: unknown) => {
      const msg = e instanceof Error ? e.message : String(e ?? "");
      const lower = msg.toLowerCase();
      return (
        lower.includes("sqlite_busy") ||
        lower.includes("database is locked") ||
        lower.includes("busy") ||
        lower.includes("timeout") ||
        lower.includes("timed out")
      );
    };

    const runOnce = async () => {
      return await db.$transaction(async (tx) => {
        const meTx = await tx.user.findUnique({ where: { id: user.id } });
        if (!meTx) throw new Error("User not found.");
        if (meTx.balance < totalCost) {
          throw new Error(
            "Insufficient balance for the start fee and your initial m².",
          );
        }

        await tx.user.update({
          where: { id: user.id },
          data: {
            balance: toCHF(meTx.balance - totalCost),
            landOwnedM2: meTx.landOwnedM2 + ownerInitialM2,
          },
        });

        const island = await tx.island.create({
          data: {
            owner: { connect: { id: user.id } },
            name,
            description,
            ticker,
            iconUrl: iconUrl,
            totalSupplyM2,
            startPriceUSD: toCHF(startPriceUSD),
            stepUpPct: toCHF(stepUpPct),
            stepDownPct: toCHF(stepDownPct),
            commissionPct: 5,
            ownerSharePct: 80,
            platformSharePct: 20,
            // soldM2 counts whole m²
            soldM2: ownerInitialM2 > 0 ? ownerInitialM2 : 0,
          },
        });

        return island;
      });
    };

    let island: any;
    let lastErr: unknown;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        island = await runOnce();
        break;
      } catch (e) {
        lastErr = e;
        if (isTransientDbError(e) && attempt < 2) {
          const msg = e instanceof Error ? e.message : String(e ?? "");
          console.log(
            "[createIsland] transient error, retrying",
            attempt + 1,
            msg.slice(0, 120),
          );
          await new Promise((r) => setTimeout(r, 150 * (attempt + 1)));
          continue;
        }
        throw e;
      }
    }
    if (!island) {
      const msg = lastErr instanceof Error ? lastErr.message : "Unknown error";
      throw new Error(msg);
    }

    if (diagId) {
      try {
        await db.createIslandDiag.update({
          where: { diagId },
          data: {
            status: "OK",
            ok: true,
            islandId: (island as any)?.id ?? null,
          },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "unknown";
        console.log("[createIsland] diag ok update failed:", msg);
      }
    }

    console.log("[createIsland] created:", (island as any)?.id);

    try {
      await db.islandPriceSample.create({
        data: { islandId: island.id, priceUSD: island.startPriceUSD },
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : "unknown";
      console.log("[createIsland] price sample failed:", msg);
    }

    return { ok: true as const, island };
  } catch (error) {
    const safeMsg = normalizeCreateIslandError(error);
    const errName = (error as any)?.name ? String((error as any).name) : "";
    const errCode = (error as any)?.code ? String((error as any).code) : "";
    const diagId = String((input as any)?.diagId ?? "").trim();

    if (diagId) {
      try {
        await db.createIslandDiag.update({
          where: { diagId },
          data: {
            status: "FAILED",
            ok: false,
            errorMessage: safeTrim(safeMsg, 600),
            errorName: safeTrim(errName, 120),
            errorCode: safeTrim(errCode, 120),
          },
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : "unknown";
        console.log("[createIsland] diag fail update failed:", msg);
      }
    }

    console.log(
      "[createIsland] failed:",
      safeMsg,
      errName ? `name=${errName}` : "",
      errCode ? `code=${errCode}` : "",
      diagId ? `diag=${diagId}` : "",
    );
    throw new Error(safeMsg);
  }
}

export async function getCreateIslandDiag(input: { diagId: string }) {
  const auth = await getAuth({ required: false });
  if (auth.status !== "authenticated" || !auth.userId) {
    throw new Error("Please sign in to continue.");
  }
  const diagId = String(input?.diagId ?? "").trim();
  if (!diagId) return null;

  const diag = await db.createIslandDiag.findUnique({ where: { diagId } });
  if (!diag) return null;

  if (diag.userId && diag.userId !== auth.userId) {
    const me = await db.user.findUnique({ where: { id: auth.userId } });
    if (!me?.isAdmin) {
      throw new Error("Kein Zugriff auf diese Diagnose.");
    }
  }

  return diag;
}

export async function listIslands() {
  try {
    const rows = await db.island.findMany({
      orderBy: { createdAt: "desc" },
      // Select only the fields the Islands UI needs (faster + smaller payload)
      select: {
        id: true,
        name: true,
        description: true,
        ticker: true,
        iconUrl: true,
        totalSupplyM2: true,
        startPriceUSD: true,
        stepUpPct: true,
        stepDownPct: true,
        commissionPct: true,
        ownerSharePct: true,
        platformSharePct: true,
        soldM2: true,
        createdAt: true,
        owner: { select: { id: true, name: true, handle: true, image: true } },
      },
    });

    const result = rows.map((r) => {
      const start = Number((r as any)?.startPriceUSD ?? 0);
      const stepUp = Number((r as any)?.stepUpPct ?? 0);
      const sold = Math.max(0, Math.floor(Number((r as any)?.soldM2 ?? 0)));

      const currentPriceUSD = _islandPriceAtSoldM2(
        {
          startPriceUSD: start,
          stepUpPct: stepUp,
          totalSupplyM2: Number((r as any)?.totalSupplyM2 ?? 1),
        },
        sold,
      );

      return {
        id: r.id,
        name: r.name,
        description: (r as any).description ?? null,
        ticker: r.ticker,
        iconUrl: r.iconUrl,
        totalSupplyM2: r.totalSupplyM2,
        startPriceUSD: r.startPriceUSD,
        currentPriceUSD,
        stepUpPct: r.stepUpPct,
        stepDownPct: r.stepDownPct,
        commissionPct: r.commissionPct,
        ownerSharePct: r.ownerSharePct,
        platformSharePct: r.platformSharePct,
        owner: r.owner,
        createdAt: r.createdAt,
      };
    });

    return result;
  } catch (error) {
    console.error("listIslands error", error);
    throw error;
  }
}

export async function updateIslandIcon(input: {
  islandId: string;
  iconBase64?: string;
  remove?: boolean;
}) {
  const normalize = (err: unknown) => {
    const msg = err instanceof Error ? err.message : String(err ?? "");
    const lower = msg.toLowerCase();
    if (
      lower.includes("missingautherror") ||
      lower.includes("unauthenticated")
    ) {
      return "Please sign in.";
    }
    if (lower.includes("not found")) return "Island not found.";
    if (lower.includes("forbidden") || lower.includes("permission")) {
      return "You are not allowed to change this island's icon.";
    }
    return msg?.trim() ? msg.trim() : "Couldn't change the island icon.";
  };

  try {
    const userId = await requireAppUserId({
      accessToken: (input as any)?.accessToken,
    });
    const user = await ensureUserWithCompliance(userId);

    const islandId = String(input?.islandId ?? "").trim();
    if (!islandId) throw new Error("Island not found.");

    const island = await db.island.findUnique({ where: { id: islandId } });
    if (!island) throw new Error("Island not found.");
    if (String((island as any).ownerId) !== String(user.id)) {
      throw new Error("Forbidden");
    }

    let iconUrl: string | null = null;
    if (!input?.remove) {
      const b64 = String(input?.iconBase64 ?? "").trim();
      if (!b64) {
        // treat empty as remove
        iconUrl = null;
      } else {
        // basic size guard (prevents accidental huge payloads)
        if (b64.length > 7_000_000) {
          throw new Error("Image is too large. Please use a smaller image.");
        }
        try {
          iconUrl = await upload({
            bufferOrBase64: b64,
            fileName: `island-icon-${islandId}-${Date.now()}.png`,
          });
        } catch (e) {
          const msg = e instanceof Error ? e.message : "unknown";
          console.log("[updateIslandIcon] upload failed:", msg);
          throw new Error("Couldn't upload the image.");
        }
      }
    }

    const updated = await db.island.update({
      where: { id: islandId },
      data: { iconUrl },
    });

    return { ok: true as const, island: updated };
  } catch (error) {
    const safe = normalize(error);
    console.log("[updateIslandIcon] failed:", safe);
    throw new Error(safe);
  }
}

export async function getIsland(input: { id: string }) {
  try {
    const id = String(input?.id ?? "");
    const r = await db.island.findUnique({
      where: { id },
      include: { owner: true },
    });
    if (!r) {
      return { island: null, priceSamples: [] as any[] };
    }

    const start = Number((r as any)?.startPriceUSD ?? 0);
    const stepUp = Number((r as any)?.stepUpPct ?? 0);
    const sold = Math.max(0, Math.floor(Number((r as any)?.soldM2 ?? 0)));

    const currentPriceUSD = _islandPriceAtSoldM2(
      {
        startPriceUSD: start,
        stepUpPct: stepUp,
        totalSupplyM2: Number((r as any)?.totalSupplyM2 ?? 1),
      },
      sold,
    );
    const samples = await db.islandPriceSample.findMany({
      where: { islandId: id },
      orderBy: { createdAt: "asc" },
      take: 200,
    });

    const enrichedIsland = { ...(r as any), currentPriceUSD };
    const normalizedSamples = [...(samples as any[])];
    const last = normalizedSamples[normalizedSamples.length - 1];
    const lastPrice = last ? Number((last as any).priceUSD ?? 0) : 0;
    if (
      normalizedSamples.length === 0 ||
      !Number.isFinite(lastPrice) ||
      Math.abs(lastPrice - currentPriceUSD) > 1e-9
    ) {
      normalizedSamples.push({
        islandId: id,
        priceUSD: currentPriceUSD,
        createdAt: new Date(),
      } as any);
    }

    return { island: enrichedIsland, priceSamples: normalizedSamples };
  } catch (error) {
    console.error("getIsland error", error);
    throw error;
  }
}

export async function getUsersBasic(input: { userIds: string[] }) {
  try {
    const ids = Array.from(
      new Set((input?.userIds ?? []).map((s) => String(s))),
    )
      .filter(Boolean)
      .slice(0, 50);

    if (ids.length === 0) return { users: [] as any[] };

    const users = await db.user.findMany({
      where: { id: { in: ids } },
      select: {
        id: true,
        name: true,
        handle: true,
        image: true,
        displayNameLocal: true,
        profileImageUrl: true,
      },
    });

    return { users };
  } catch (error) {
    console.error("getUsersBasic error", error);
    throw error;
  }
}

export async function getMyIslandPurchaseSummary(input: { islandId: string }) {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    const islandId = String(input?.islandId ?? "");
    if (!islandId) return { totalBoughtM2: 0, totalSpentPPC: 0 };

    // We can't filter by islandId in SQL because it's stored inside Transaction.meta.
    // So we fetch a bounded set of BUY and SECONDARY_BUY transactions and filter in memory.
    const rows = await db.transaction.findMany({
      where: {
        userId: auth.userId,
        type: { in: ["BUY", "SECONDARY_BUY"] },
      },
      orderBy: { createdAt: "desc" },
      take: 5000,
      select: { m2: true, totalAmountCHF: true, meta: true },
    });

    let totalBoughtM2 = 0;
    let totalSpentPPC = 0;

    for (const r of rows) {
      const meta = _txMetaParse<{ islandId?: string | null }>(r.meta);
      if (!meta?.islandId) continue;
      if (String(meta.islandId) !== islandId) continue;
      totalBoughtM2 += Number(r.m2 ?? 0) || 0;
      totalSpentPPC += Number(r.totalAmountCHF ?? 0) || 0;
    }

    return {
      totalBoughtM2: toCHF(totalBoughtM2),
      totalSpentPPC: toCHF(totalSpentPPC),
    };
  } catch (error) {
    console.error("getMyIslandPurchaseSummary error", error);
    throw error;
  }
}

export async function getMyIslandEarningsSummary(input: {
  islandId: string;
  accessToken?: string;
}) {
  try {
    const islandId = String(input?.islandId ?? "");
    if (!islandId)
      return {
        boughtM2: 0,
        soldM2: 0,
        currentOwnedM2: 0,
        spentPPC: 0,
        proceedsPPC: 0,
        commissionEarnedPPC: 0,
        netPPC: 0,
      };

    const userId = await requireAppUserId({ accessToken: input?.accessToken });

    // Fetch a bounded set of relevant transactions and filter by islandId from meta.
    const rows = await db.transaction.findMany({
      where: {
        userId: userId,
        type: {
          in: [
            "BUY",
            "SECONDARY_BUY",
            "SECONDARY_SOLD",
            "COMMISSION_IN",
            "COMMISSION_OUT",
          ],
        },
      },
      orderBy: { createdAt: "desc" },
      take: 8000,
      select: { type: true, m2: true, totalAmountCHF: true, meta: true },
    });

    let boughtM2 = 0;
    let spentPPC = 0;
    let soldM2 = 0;
    let proceedsPPC = 0;
    let commissionEarnedPPC = 0;

    for (const r of rows) {
      const meta = _txMetaParse<{ islandId?: string | null }>(r.meta);
      if (!meta?.islandId) continue;
      if (String(meta.islandId) !== islandId) continue;

      const amt = Number(r.totalAmountCHF ?? 0) || 0;
      const m2 = Number(r.m2 ?? 0) || 0;

      if (r.type === "BUY" || r.type === "SECONDARY_BUY") {
        boughtM2 += m2;
        spentPPC += amt;
      }

      if (r.type === "SECONDARY_SOLD") {
        soldM2 += m2;
        proceedsPPC += amt;
      }

      if (r.type === "COMMISSION_IN") {
        commissionEarnedPPC += amt;
      }

      // COMMISSION_OUT is money you paid (as seller). It reduces your net.
      if (r.type === "COMMISSION_OUT") {
        proceedsPPC -= amt;
      }
    }

    const currentOwnedM2 = Math.max(0, Math.round(boughtM2 - soldM2));
    const netPPC = proceedsPPC + commissionEarnedPPC - spentPPC;

    return {
      boughtM2: Math.max(0, Math.round(boughtM2)),
      soldM2: Math.max(0, Math.round(soldM2)),
      currentOwnedM2,
      spentPPC: toCHF(spentPPC),
      proceedsPPC: toCHF(proceedsPPC),
      commissionEarnedPPC: toCHF(commissionEarnedPPC),
      netPPC: toCHF(netPPC),
    };
  } catch (error) {
    console.error("getMyIslandEarningsSummary error", error);
    throw error;
  }
}

export async function getIslandTopHolders(input: {
  islandId: string;
  limit?: number;
}) {
  try {
    const islandId = String(input?.islandId ?? "");
    const limit = Math.max(
      1,
      Math.min(100, Math.floor(Number(input?.limit ?? 10))),
    );
    if (!islandId) {
      return { totalOwnedM2: 0, holders: [] as any[] };
    }

    // We store islandId inside Transaction.meta (string). We narrow down the scan with a fast substring filter.
    const rows = await db.transaction.findMany({
      where: {
        type: { in: ["BUY", "SECONDARY_BUY", "SECONDARY_SOLD"] },
        meta: { contains: islandId },
      },
      orderBy: { createdAt: "desc" },
      take: 50000,
      select: { userId: true, type: true, m2: true, meta: true },
    });

    const ownedByUser: Record<string, number> = {};

    for (const r of rows) {
      const meta = _txMetaParse<{ islandId?: string | null }>(r.meta);
      if (!meta?.islandId) continue;
      if (String(meta.islandId) !== islandId) continue;

      const m2 = Number(r.m2 ?? 0) || 0;
      if (!r.userId) continue;

      if (r.type === "BUY" || r.type === "SECONDARY_BUY") {
        ownedByUser[r.userId] = (ownedByUser[r.userId] ?? 0) + m2;
      }

      if (r.type === "SECONDARY_SOLD") {
        ownedByUser[r.userId] = (ownedByUser[r.userId] ?? 0) - m2;
      }
    }

    const rowsAgg = Object.entries(ownedByUser)
      .map(([userId, ownedM2]) => ({
        userId,
        ownedM2: Math.max(0, Math.round(ownedM2)),
      }))
      .filter((x) => x.ownedM2 > 0)
      .sort((a, b) => b.ownedM2 - a.ownedM2);

    const totalOwnedM2 = rowsAgg.reduce((s, x) => s + x.ownedM2, 0);
    const top = rowsAgg.slice(0, limit);

    const users = await db.user.findMany({
      where: { id: { in: top.map((t) => t.userId) } },
      select: {
        id: true,
        name: true,
        handle: true,
        image: true,
        displayNameLocal: true,
        profileImageUrl: true,
      },
    });

    const userById: Record<string, any> = {};
    for (const u of users) userById[u.id] = u;

    return {
      totalOwnedM2,
      holders: top.map((t) => ({
        userId: t.userId,
        ownedM2: t.ownedM2,
        user: userById[t.userId] ?? null,
      })),
    };
  } catch (error) {
    console.error("getIslandTopHolders error", error);
    throw error;
  }
}

export async function deleteIsland(input: {
  id: string;
  accessToken?: string;
}) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    const id = String(input?.id ?? "");
    const island = await db.island.findUnique({ where: { id } });
    if (!island) {
      throw new Error("Island not found.");
    }
    if (island.ownerId !== userId) {
      throw new Error("You can only delete your own islands.");
    }

    await db.islandPriceSample.deleteMany({ where: { islandId: id } });
    await db.island.delete({ where: { id } });

    return { ok: true as const };
  } catch (error) {
    console.error("deleteIsland error", error);
    throw error;
  }
}

// Price history API
export async function getPriceHistory(input?: { minutes?: number }) {
  const minutesRaw = Number(input?.minutes ?? 60);
  const minutes = Number.isFinite(minutesRaw)
    ? Math.max(1, Math.min(minutesRaw, 60 * 24 * 365 * 30))
    : 60;

  const since = new Date(Date.now() - minutes * 60 * 1000);
  const now = new Date();

  // Keep this endpoint fast: the UI can render smoothly with a limited number of points.
  const MAX_POINTS = 1800;
  const QUERY_TIMEOUT_MS = 6_000;

  const samplesDesc = await Promise.race([
    db.marketPriceSample.findMany({
      where: { createdAt: { gte: since } },
      orderBy: { createdAt: "desc" },
      take: MAX_POINTS,
      select: { createdAt: true, price: true },
    }),
    new Promise<null>((resolve) =>
      setTimeout(() => resolve(null), QUERY_TIMEOUT_MS),
    ),
  ]).catch(() => null);

  const samples = (samplesDesc ? samplesDesc.slice().reverse() : []) as Array<{
    createdAt: Date;
    price: number;
  }>;

  if (samples.length >= 2) {
    // If we had to truncate, add an anchor point at the requested start.
    const out = samples.map((s) => ({ t: s.createdAt, price: toCHF(s.price) }));
    const firstT = out[0]?.t ? +new Date(out[0]!.t) : NaN;
    const sinceT = +since;
    if (Number.isFinite(firstT) && firstT > sinceT + 60_000) {
      out.unshift({ t: since, price: out[0]!.price });
    }
    // Ensure we always have a point at "now" so the chart can align to current market price.
    const lastT = out[out.length - 1]?.t
      ? +new Date(out[out.length - 1]!.t)
      : NaN;
    if (Number.isFinite(lastT) && +now - lastT > 60_000) {
      out.push({ t: now, price: out[out.length - 1]!.price });
    }
    return out;
  }

  if (samples.length === 1) {
    const p = toCHF(samples[0]!.price);
    return [
      { t: since, price: p },
      { t: now, price: p },
    ];
  }

  // Fallback: try a quick snapshot. If even that fails, return a stable baseline.
  const snapshot = await Promise.race([
    getPricingSnapshot(),
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 2_500)),
  ]).catch(() => null);

  const p = snapshot ? toCHF((snapshot as any).currentPrice) : 0;
  return [
    { t: since, price: p },
    { t: now, price: p },
  ];
}

export async function getNewsStats(input?: {
  range?: "1h" | "24h" | "7d" | "30d" | "all";
}) {
  const range = input?.range ?? "24h";

  const rangeMinutesByKey: Record<string, number | null> = {
    "1h": 60,
    "24h": 24 * 60,
    "7d": 7 * 24 * 60,
    "30d": 30 * 24 * 60,
    all: null,
  };

  const minutes = rangeMinutesByKey[range] ?? 24 * 60;
  const since =
    minutes === null ? null : new Date(Date.now() - minutes * 60 * 1000);
  const whereRange = since ? { createdAt: { gte: since } } : undefined;

  const snapshot = await getPricingSnapshot();
  const current = toCHF(snapshot.currentPrice);

  const [rangeAgg, rangeFirst, rangeLast, allAgg] = await Promise.all([
    db.marketPriceSample.aggregate({
      where: whereRange,
      _min: { price: true },
      _max: { price: true },
    }),
    db.marketPriceSample.findFirst({
      where: whereRange,
      orderBy: { createdAt: "asc" },
    }),
    db.marketPriceSample.findFirst({
      where: whereRange,
      orderBy: { createdAt: "desc" },
    }),
    db.marketPriceSample.aggregate({
      _min: { price: true },
      _max: { price: true },
    }),
  ]);

  const open = toCHF(Number(rangeFirst?.price ?? current));
  const close = toCHF(Number(rangeLast?.price ?? current));

  const rangeHigh = toCHF(Number(rangeAgg._max.price ?? current));
  const rangeLow = toCHF(Number(rangeAgg._min.price ?? current));

  const allTimeHigh = toCHF(Number(allAgg._max.price ?? current));
  const allTimeLow = toCHF(Number(allAgg._min.price ?? current));

  const changeAbs = toCHF(close - open);
  const changePct = open > 0 ? toCHF(((close - open) / open) * 100) : 0;

  const updatedAt = rangeLast?.createdAt ?? new Date();

  return {
    range,
    rangeMinutes: minutes,
    since,
    updatedAt,

    currentPricePerM2: current,

    rangeOpen: open,
    rangeClose: close,
    rangeHigh,
    rangeLow,
    changeAbs,
    changePct,

    allTimeHigh,
    allTimeLow,
  };
}

export async function getTrendingArea(input?: { minutes?: number }) {
  void input;
  return null;
}

export async function listNewsPosts(input?: { limit?: number }) {
  const limit = Math.max(1, Math.min(50, Number(input?.limit ?? 20)));
  return await db.newsPost.findMany({
    take: limit,
    orderBy: { createdAt: "desc" },
    select: {
      id: true,
      title: true,
      body: true,
      createdAt: true,
      updatedAt: true,
      createdById: true,
    },
  });
}

export async function getNewsPost(input: { id: string }) {
  const id = String(input?.id ?? "").trim();
  if (!id) throw new Error("Missing id");

  const post = await db.newsPost.findUnique({
    where: { id },
    select: {
      id: true,
      title: true,
      body: true,
      createdAt: true,
      updatedAt: true,
      createdById: true,
    },
  });

  if (!post) throw new Error("News not found.");
  return post;
}

export async function createNewsPost(input: {
  title: string;
  body: string;
  accessToken?: string;
}) {
  const token = (input?.accessToken || "").trim();

  let actorUserId = "";

  if (token) {
    let email = "";
    try {
      const supabase = requireSupabaseClient();
      const res = await supabase.auth.getUser(token);
      const u = (res as any)?.data?.user;
      actorUserId = u?.id ? String(u.id) : "";
      email = u?.email ? String(u.email) : "";
    } catch {
      throw new Error("Please sign in to continue.");
    }

    if (!actorUserId) {
      throw new Error("Please sign in to continue.");
    }

    if (!email || email.toLowerCase() !== OWNER_EMAIL.toLowerCase()) {
      throw new Error("Not allowed.");
    }
  } else {
    const auth = await getAuth({ required: true });
    if (!auth.userId) throw new Error("Please sign in to continue.");
    if (auth.userId !== OWNER_USER_ID) throw new Error("Not allowed.");
    actorUserId = auth.userId;
  }

  await ensureUser(actorUserId);

  const title = String(input?.title ?? "").trim();
  const body = String(input?.body ?? "").trim();
  if (!title) throw new Error("Please enter a title.");
  if (!body) throw new Error("Please enter a description.");

  return await db.newsPost.create({
    data: {
      title,
      body,
      createdById: actorUserId,
    },
    select: {
      id: true,
      title: true,
      body: true,
      createdAt: true,
      updatedAt: true,
      createdById: true,
    },
  });
}

export async function deleteNewsPost(input: {
  id: string;
  accessToken?: string;
}) {
  const token = (input?.accessToken || "").trim();

  let actorUserId = "";

  if (token) {
    let email = "";
    try {
      const supabase = requireSupabaseClient();
      const res = await supabase.auth.getUser(token);
      const u = (res as any)?.data?.user;
      actorUserId = u?.id ? String(u.id) : "";
      email = u?.email ? String(u.email) : "";
    } catch {
      throw new Error("Please sign in to continue.");
    }

    if (!actorUserId) {
      throw new Error("Please sign in to continue.");
    }

    if (!email || email.toLowerCase() !== OWNER_EMAIL.toLowerCase()) {
      throw new Error("Not allowed.");
    }
  } else {
    const auth = await getAuth({ required: true });
    if (!auth.userId) throw new Error("Please sign in to continue.");
    if (auth.userId !== OWNER_USER_ID) throw new Error("Not allowed.");
    actorUserId = auth.userId;
  }

  await ensureUser(actorUserId);

  const id = String(input?.id ?? "").trim();
  if (!id) throw new Error("Missing id");

  await db.newsPost.delete({ where: { id } });
  return { ok: true as const };
}

export async function _autoNewsCronHandler() {
  const ownerId = OWNER_USER_ID;
  await ensureUser(ownerId);

  const now = Date.now();
  const TWO_HOURS_MS = 2 * 60 * 60 * 1000;
  const NINETY_MIN_MS = 90 * 60 * 1000;

  const lastAuto = await db.newsPost.findFirst({
    where: { isAuto: true },
    orderBy: { createdAt: "desc" },
    select: { createdAt: true },
  });

  if (lastAuto?.createdAt) {
    const lastAt = new Date(lastAuto.createdAt).getTime();
    if (now - lastAt < TWO_HOURS_MS) {
      return { ok: true as const, skipped: true as const, reason: "recent" };
    }
  }

  const snapshot = await getPricingSnapshot();
  const current = toCHF(snapshot.currentPrice);

  const allAgg = await db.marketPriceSample.aggregate({
    _max: { price: true },
  });
  const allTimeHigh = toCHF(Number(allAgg._max.price ?? current));

  const since24h = new Date(now - 24 * 60 * 60 * 1000);
  const [rangeFirst, rangeLast] = await Promise.all([
    db.marketPriceSample.findFirst({
      where: { createdAt: { gte: since24h } },
      orderBy: { createdAt: "asc" },
    }),
    db.marketPriceSample.findFirst({
      where: { createdAt: { gte: since24h } },
      orderBy: { createdAt: "desc" },
    }),
  ]);

  const open = toCHF(Number(rangeFirst?.price ?? current));
  const close = toCHF(Number(rangeLast?.price ?? current));
  const changeAbs = toCHF(close - open);
  const changePct = open > 0 ? toCHF(((close - open) / open) * 100) : 0;

  const since2h = new Date(now - TWO_HOURS_MS);
  const newIslands = await db.island.findMany({
    where: { createdAt: { gte: since2h } },
    orderBy: { createdAt: "desc" },
    take: 3,
    select: { name: true, ticker: true },
  });

  const fmt = (n: any) => `${Number(n ?? 0).toFixed(2)} PPC/m²`;
  const fmtAbs = (n: any) => `${Number(n ?? 0).toFixed(2)} PPC`;
  const fmtPct = (n: any) => {
    const v = Number(n ?? 0);
    const sign = v > 0 ? "+" : "";
    return `${sign}${v.toFixed(2)}%`;
  };

  let autoTopic = "MARKET_UPDATE";
  let title = "Market update";
  let body = `Current price: ${fmt(current)}\nLast 24h: ${fmtPct(changePct)} (${fmtAbs(changeAbs)})\nAll-time high: ${fmt(allTimeHigh)}\n\nOpen Market to explore.`;

  const isAthNow = current >= allTimeHigh - 0.01;
  if (isAthNow) {
    autoTopic = "ATH";
    title = "All-time high m² price";
    body = `New record: ${fmt(allTimeHigh)}.\nCurrent: ${fmt(current)}\n\nOpen Market to explore.`;
  } else if (newIslands.length > 0) {
    autoTopic = "NEW_ISLANDS";
    title = "New islands";
    const lines = newIslands.map((i) =>
      i.ticker ? `• ${i.name} (${i.ticker})` : `• ${i.name}`,
    );
    body = `New islands are live:\n${lines.join("\n")}\n\nOpen Islands to explore performance and buy or sell shares.`;
  }

  const recentSameTopic = await db.newsPost.findFirst({
    where: {
      isAuto: true,
      autoTopic,
      createdAt: { gte: new Date(now - NINETY_MIN_MS) },
    },
    select: { id: true },
  });

  if (recentSameTopic) {
    return { ok: true as const, skipped: true as const, reason: "duplicate" };
  }

  const post = await db.newsPost.create({
    data: {
      title,
      body,
      isAuto: true,
      autoTopic,
      createdById: ownerId,
    },
    select: {
      id: true,
      title: true,
      body: true,
      createdAt: true,
      updatedAt: true,
      createdById: true,
    },
  });

  return { ok: true as const, skipped: false as const, post };
}

// Internal seed to ensure market exists (runs once automatically by platform if needed)
export async function _seedMarket() {
  await ensureMarket();
  return { ok: true as const };
}

// Global reset seed: set world to baseline (400,000 m², base price 80 EUR),
// clear all plots/houses/certificates/transactions and reset users' land/balance.
// Note: This is a seed so it will run once to bring the world to the requested zero state.
export async function _seedResetWorldBaseline() {
  const market = await ensureMarket();

  // Wipe world data
  await db.certificate.deleteMany({});
  await db.plot.deleteMany({});
  await db.houseHolding.deleteMany({});
  await db.transaction.deleteMany({});
  await db.marketPriceSample.deleteMany({});

  // Reset users to zero land and zero balance
  await db.user.updateMany({ data: { landOwnedM2: 0, balance: 0 } });

  // Reset market parameters
  await db.market.update({
    where: { id: market.id },
    data: {
      totalSupplyM2: _countLandCells(),
      cumulativeSoldM2: 0,
      basePrice: 80,
      currentPrice: 80,
      pricePressure: 0,
      pressureUpdatedAt: new Date(),
      curvingStartAt: null,
      activePurchaseDays: 0,
      lastActivePurchaseDay: null,
      sellPressureM2: 0,
      priceDropPct: 0,
    },
  });
  await broadcastMarket();
  return { ok: true as const };
}

// Seed: ensure the app owner is admin
export async function _seedEnsureOwnerAdmin() {
  try {
    const ownerId = OWNER_USER_ID;
    let user = await db.user.findUnique({ where: { id: ownerId } });
    if (!user) {
      user = await db.user.create({
        data: {
          id: ownerId,
          balance: 0,
          landOwnedM2: 0,
          isAdmin: true,
        },
      });
    } else if (!user.isAdmin) {
      await db.user.update({ where: { id: ownerId }, data: { isAdmin: true } });
    }
    return { ok: true as const };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("_seedEnsureOwnerAdmin error", _e);
    throw _e;
  }
}

// One-time setup helper: can only be called by the platform/agent (internal endpoint)
export async function _setInitialAdmin(userId: string) {
  if (!userId) throw new Error("Missing userId");
  await db.user.upsert({
    where: { id: userId },
    create: { id: userId, isAdmin: true },
    update: { isAdmin: true },
  });
  return { ok: true as const };
}

// FX: Live exchange rates with 30-minute cache using exchangerate.host (no API key)
export async function getFxRates(input?: { base?: string }) {
  const base = (input?.base || "EUR").toUpperCase();
  try {
    const cache = await db.fxRateCache
      .findUnique({ where: { base } })
      .catch(() => null as any);
    const now = Date.now();
    const maxAgeMs = 30 * 60 * 1000;
    if (cache && new Date(cache.fetchedAt).getTime() > now - maxAgeMs) {
      try {
        const parsed = JSON.parse(cache.ratesJson) as Record<string, number>;
        return { base, rates: parsed, fetchedAt: cache.fetchedAt };
      } catch {}
    }

    const res = await _fetchWithTimeout(
      `https://api.exchangerate.host/latest?base=${encodeURIComponent(base)}`,
    );
    if (!res.ok) throw new Error(`FX fetch failed: ${res.status}`);
    const json = (await res.json()) as { rates: Record<string, number> };
    const rates = json?.rates || {};

    const row = await db.fxRateCache.upsert({
      where: { base },
      update: { ratesJson: JSON.stringify(rates), fetchedAt: new Date() },
      create: { base, ratesJson: JSON.stringify(rates) },
    });
    return { base, rates, fetchedAt: row.fetchedAt };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("getFxRates error", _e);
    // Fallback: minimal identity rate to avoid breaking UI
    return { base, rates: { [base]: 1 }, fetchedAt: new Date() };
  }
}

function _fmtISODate(d: Date) {
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// FX: small historical series for charts (cached)
export async function getFxSeries(input: {
  base: string;
  quote: string;
  days?: number;
}) {
  const base = String(input?.base || "USD").toUpperCase();
  const quote = String(input?.quote || "CHF").toUpperCase();
  const daysRaw = Number(input?.days ?? 30);
  const days = Number.isFinite(daysRaw)
    ? Math.min(Math.max(daysRaw, 2), 120)
    : 30;

  const key = `${base}:${quote}:${days}d`;
  const now = Date.now();
  const maxAgeMs = 10 * 60 * 1000;

  try {
    const cache = await db.fxRateSeriesCache
      .findUnique({ where: { key } })
      .catch(() => null as any);

    if (cache && new Date(cache.fetchedAt).getTime() > now - maxAgeMs) {
      try {
        const points = JSON.parse(cache.pointsJson) as Array<{
          t: string;
          v: number;
        }>;
        if (Array.isArray(points) && points.length >= 2) {
          return {
            base,
            quote,
            days,
            points,
            fetchedAt: cache.fetchedAt,
          };
        }
      } catch {}
    }

    const end = new Date();
    const start = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    const startStr = _fmtISODate(start);
    const endStr = _fmtISODate(end);

    // Prefer frankfurter.app for historical rates (no API key).
    // Docs: https://www.frankfurter.app/docs/
    const url = `https://api.frankfurter.app/${startStr}..${endStr}?from=${encodeURIComponent(
      base,
    )}&to=${encodeURIComponent(quote)}`;
    const res = await _fetchWithTimeout(url);
    if (!res.ok) throw new Error(`FX series fetch failed: ${res.status}`);

    const json = (await res.json()) as {
      rates?: Record<string, Record<string, number>>;
    };

    const ratesByDay = json?.rates || {};
    const points = Object.keys(ratesByDay)
      .sort()
      .map((day) => ({
        t: day,
        v: Number((ratesByDay as any)?.[day]?.[quote]),
      }))
      .filter((p) => Number.isFinite(p.v));

    if (points.length < 2) {
      throw new Error("FX series returned too few points");
    }

    const row = await db.fxRateSeriesCache.upsert({
      where: { key },
      update: {
        base,
        quote,
        days,
        pointsJson: JSON.stringify(points),
        fetchedAt: new Date(),
      },
      create: { key, base, quote, days, pointsJson: JSON.stringify(points) },
    });

    return { base, quote, days, points, fetchedAt: row.fetchedAt };
  } catch (_e) {
    /* eslint-disable-line @typescript-eslint/no-unused-vars */
    console.error("getFxSeries error", _e);

    // Fallback: derive a flat line from the latest rate so UI stays usable.
    const latest = await getFxRates({ base });
    const v = Number((latest?.rates as any)?.[quote] ?? 1);
    const safe = Number.isFinite(v) ? v : 1;
    const end = new Date();
    const points = Array.from({ length: Math.max(days, 2) }, (_, i) => {
      const d = new Date(end.getTime() - (days - 1 - i) * 24 * 60 * 60 * 1000);
      return { t: _fmtISODate(d), v: safe };
    });

    return { base, quote, days, points, fetchedAt: new Date() };
  }
}

// Helper: pick a free anchor plot near the island center
async function _pickFirstFreePlotAnchor(): Promise<{ x: number; y: number }> {
  // start from island center and spiral out until a free, allowed cell is found
  const cx = ISLAND_X0 + Math.floor(ISLAND_W / 2);
  const cy = ISLAND_Y0 + Math.floor(ISLAND_H / 2);
  const maxR = Math.max(ISLAND_W, ISLAND_H);
  const isFree = async (x: number, y: number) => {
    if (!_isAllowedLot(x, y)) return false;
    const existing = await db.plot.findFirst({ where: { x, y } });
    return !existing;
  };
  // radius 0..maxR
  for (let r = 0; r < maxR; r++) {
    // iterate the ring border only for efficiency
    for (let dx = -r; dx <= r; dx++) {
      const x1 = cx + dx;
      const y1 = cy - r;
      const y2 = cy + r;
      if (await isFree(x1, y1)) return { x: x1, y: y1 };
      if (r > 0 && (await isFree(x1, y2))) return { x: x1, y: y2 };
    }
    for (let dy = -r + 1; dy <= r - 1; dy++) {
      const y1 = cy + dy;
      const x1 = cx - r;
      const x2 = cx + r;
      if (await isFree(x1, y1)) return { x: x1, y: y1 };
      if (r > 0 && (await isFree(x2, y1))) return { x: x2, y: y1 };
    }
  }
  // fallback to center if none (should not happen)
  return { x: cx, y: cy };
}

export async function listMyPlots(input?: { accessToken?: string }) {
  try {
    // Prefer Supabase auth when provided, to keep identity consistent with buys.
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    await ensureUser(userId);
    const plots = await db.plot.findMany({
      where: { ownerId: userId },
      select: { x: true, y: true, createdAt: true },
      orderBy: { createdAt: "asc" },
    });
    return plots.map((p) => ({ x: p.x, y: p.y, createdAt: p.createdAt }));
  } catch (error) {
    console.error("listMyPlots error", error);
    throw error;
  }
}

export async function getMyAnchorPlot(input?: { accessToken?: string }) {
  try {
    const userId = await requireAppUserId({ accessToken: input?.accessToken });
    await ensureUser(userId);
    const p = await db.plot.findFirst({
      where: { ownerId: userId },
      orderBy: [{ createdAt: "asc" }],
      select: { x: true, y: true },
    });
    if (!p) return null;
    return { x: p.x, y: p.y };
  } catch (error) {
    console.error("getMyAnchorPlot error", error);
    throw error;
  }
}
export async function buyM2(input: { quantity: number }) {
  const supabase = requireSupabaseClient() as any;

  const { data, error } = await supabase.rpc("buy_m2", {
    p_quantity: input.quantity,
  });

  if (error) throw error;

  return data;
}

// Admin: Reset world to initial state (wipe plots/houses/certificates/tx, reset users and market)
export async function adminResetWorld(input: { confirm: string }) {
  try {
    const auth = await getAuth({ required: false });
    if (auth.status !== "authenticated" || !auth.userId) {
      throw new Error("Please sign in to continue.");
    }
    const me = await ensureUser(auth.userId);

    if (!me.isAdmin) {
      throw new Error("Only the owner can reset the world.");
    }

    if ((input?.confirm ?? "").toUpperCase() !== "RESET") {
      throw new Error("Confirm by typing RESET");
    }

    const market = await ensureMarket();

    // Wipe world data in a transaction-like sequence
    await db.certificate.deleteMany({});
    await db.plot.deleteMany({});
    await db.houseHolding.deleteMany({});
    await db.transaction.deleteMany({});
    await db.marketPriceSample.deleteMany({});

    // Reset users
    await db.user.updateMany({
      data: { landOwnedM2: 0, balance: 0 },
    });

    // Reset market to baseline and full supply based on island land cells
    await db.market.update({
      where: { id: market.id },
      data: {
        totalSupplyM2: _countLandCells(),
        cumulativeSoldM2: 0,
        basePrice: 80,
        currentPrice: 80,
        pricePressure: 0,
        pressureUpdatedAt: new Date(),
        // Curving-Line Felder auf Anfang
        curvingStartAt: null,
        activePurchaseDays: 0,
        lastActivePurchaseDay: null,
        sellPressureM2: 0,
        priceDropPct: 0,
      },
    });

    await broadcastMarket();

    return { ok: true as const };
  } catch (error) {
    console.error("adminResetWorld error", error);
    throw error;
  }
}
// ===== Cloudflare Worker Buy API =====
