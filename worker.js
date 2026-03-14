require("dotenv").config({ quiet: true });

const mqtt = require("mqtt");
const { Pool } = require("pg");

const REQUIRED_ENV = [
  "MQTT_URL",
  "MQTT_USERNAME",
  "MQTT_PASSWORD",
  "PGHOST",
  "PGPORT",
  "PGDATABASE",
  "PGUSER",
  "PGPASSWORD",
];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`[ENV] Missing ${key}`);
    process.exit(1);
  }
}

const pool = new Pool({
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  database: process.env.PGDATABASE,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  ssl: process.env.PGSSLMODE === "require" ? { rejectUnauthorized: false } : false,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

const mqttClient = mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  clientId: process.env.MQTT_CLIENT_ID || `9cc-neon-worker-${Math.random().toString(16).slice(2, 8)}`,
  reconnectPeriod: 3000,
  connectTimeout: 10000,
  clean: true,
});

const TOPICS = [
  "9CC/+/+/status",
  "9CC/+/+/points",
  "9CC/+/+/event",
  // backward compatibility
  "9CC/+/+/telemetry",
];

function safeJsonParse(text) {
  try {
    return JSON.parse(text);
  } catch (err) {
    return null;
  }
}

function topicParts(topic) {
  const parts = String(topic).split("/");
  return {
    root: parts[0] || "",
    site: parts[1] || "",
    deviceId: parts[2] || "",
    sub: parts[3] || "",
  };
}

function parseTs(value) {
  if (!value || typeof value !== "string") return new Date();
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? new Date() : d;
}

function inferPointMeta(pointKey, value) {
  let pointType = "generic";
  let unit = null;

  if (pointKey.startsWith("temp")) {
    pointType = "temp";
    unit = "C";
  } else if (pointKey.startsWith("hum")) {
    pointType = "humidity";
    unit = "%";
  } else if (pointKey.startsWith("output")) {
    pointType = "output";
  } else if (pointKey.startsWith("input")) {
    pointType = "input";
  }

  let dataType = "text";
  if (typeof value === "number") dataType = "number";
  else if (typeof value === "boolean") dataType = "boolean";

  let channelNo = null;
  const m = pointKey.match(/(\d+)$/);
  if (m) channelNo = Number(m[1]);

  return { pointType, dataType, unit, channelNo };
}

async function upsertDeviceStatus(client, payload, fallback) {
  const ts = parseTs(payload.now);
  const sql = `
    INSERT INTO devices (
      device_id,
      device_name,
      site,
      firmware,
      mac_address,
      ip,
      online,
      time_synced,
      safe_mode,
      safe_reason,
      mqtt_status,
      mqtt_use_tls,
      relay_type,
      uptime_sec,
      free_heap,
      last_seen,
      status_payload
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb
    )
    ON CONFLICT (device_id)
    DO UPDATE SET
      device_name   = EXCLUDED.device_name,
      site          = EXCLUDED.site,
      firmware      = EXCLUDED.firmware,
      mac_address   = EXCLUDED.mac_address,
      ip            = EXCLUDED.ip,
      online        = EXCLUDED.online,
      time_synced   = EXCLUDED.time_synced,
      safe_mode     = EXCLUDED.safe_mode,
      safe_reason   = EXCLUDED.safe_reason,
      mqtt_status   = EXCLUDED.mqtt_status,
      mqtt_use_tls  = EXCLUDED.mqtt_use_tls,
      relay_type    = EXCLUDED.relay_type,
      uptime_sec    = EXCLUDED.uptime_sec,
      free_heap     = EXCLUDED.free_heap,
      last_seen     = EXCLUDED.last_seen,
      status_payload = EXCLUDED.status_payload
  `;

  const values = [
    payload.deviceId || fallback.deviceId,
    payload.deviceName || null,
    payload.site || fallback.site,
    payload.fw || null,
    payload.mac || null,
    payload.ip || null,
    payload.online ?? false,
    payload.timeSynced ?? null,
    payload.safeMode ?? null,
    payload.safeReason || null,
    payload.mqttStatus || payload.mqtt || null,
    payload.mqttUseTls ?? null,
    payload.relayType || null,
    payload.uptimeSec ?? null,
    payload.freeHeap ?? null,
    ts,
    JSON.stringify(payload),
  ];

  await client.query(sql, values);
}

async function ensureDeviceExists(client, payload, fallback) {
  const sql = `
    INSERT INTO devices (
      device_id,
      device_name,
      site,
      last_seen,
      status_payload
    )
    VALUES ($1,$2,$3,$4,$5::jsonb)
    ON CONFLICT (device_id)
    DO UPDATE SET
      device_name = COALESCE(EXCLUDED.device_name, devices.device_name),
      site = COALESCE(EXCLUDED.site, devices.site),
      last_seen = EXCLUDED.last_seen
  `;

  await client.query(sql, [
    payload.deviceId || fallback.deviceId,
    payload.deviceName || null,
    payload.site || fallback.site,
    parseTs(payload.ts || payload.now),
    JSON.stringify(payload),
  ]);
}

async function upsertDevicePoint(client, deviceId, pointKey, value, ts) {
  const meta = inferPointMeta(pointKey, value);

  const sql = `
    INSERT INTO device_points (
      device_id,
      point_key,
      point_name,
      point_type,
      data_type,
      unit,
      channel_no,
      last_seen
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (device_id, point_key)
    DO UPDATE SET
      point_name = EXCLUDED.point_name,
      point_type = EXCLUDED.point_type,
      data_type = EXCLUDED.data_type,
      unit = EXCLUDED.unit,
      channel_no = EXCLUDED.channel_no,
      last_seen = EXCLUDED.last_seen
  `;

  await client.query(sql, [
    deviceId,
    pointKey,
    pointKey,
    meta.pointType,
    meta.dataType,
    meta.unit,
    meta.channelNo,
    ts,
  ]);
}

async function upsertPointLatest(client, deviceId, pointKey, value, ts, sourceTopic) {
  const valueNum = typeof value === "number" ? value : null;
  const valueBool = typeof value === "boolean" ? value : null;
  const valueText = typeof value === "string" ? value : null;

  const sql = `
    INSERT INTO point_latest (
      device_id,
      point_key,
      ts,
      value_num,
      value_bool,
      value_text,
      source_topic
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (device_id, point_key)
    DO UPDATE SET
      ts = EXCLUDED.ts,
      value_num = EXCLUDED.value_num,
      value_bool = EXCLUDED.value_bool,
      value_text = EXCLUDED.value_text,
      source_topic = EXCLUDED.source_topic
  `;

  await client.query(sql, [
    deviceId,
    pointKey,
    ts,
    valueNum,
    valueBool,
    valueText,
    sourceTopic,
  ]);
}

async function insertPointHistory(client, deviceId, pointKey, value, ts, sourceTopic) {
  const valueNum = typeof value === "number" ? value : null;
  const valueBool = typeof value === "boolean" ? value : null;
  const valueText = typeof value === "string" ? value : null;

  const sql = `
    INSERT INTO point_history (
      ts,
      device_id,
      point_key,
      value_num,
      value_bool,
      value_text,
      source_topic
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7)
  `;

  await client.query(sql, [
    ts,
    deviceId,
    pointKey,
    valueNum,
    valueBool,
    valueText,
    sourceTopic,
  ]);
}

async function insertEventLog(client, payload, fallback) {
  const sql = `
    INSERT INTO device_event_log (
      ts,
      device_id,
      device_name,
      site,
      event_type,
      severity,
      message,
      raw_payload
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
  `;

  await client.query(sql, [
    parseTs(payload.ts),
    payload.deviceId || fallback.deviceId,
    payload.deviceName || null,
    payload.site || fallback.site,
    payload.eventType || "unknown",
    payload.severity || "info",
    payload.message || null,
    JSON.stringify(payload),
  ]);
}

async function handleStatus(payload, fallback) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await upsertDeviceStatus(client, payload, fallback);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function handlePoints(payload, fallback, sourceTopic) {
  const deviceId = payload.deviceId || fallback.deviceId;
  const ts = parseTs(payload.ts);

  if (!deviceId) throw new Error("Missing deviceId in points payload");
  if (!payload.points || typeof payload.points !== "object" || Array.isArray(payload.points)) {
    throw new Error("Invalid points payload");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await ensureDeviceExists(client, payload, fallback);

    for (const [pointKey, value] of Object.entries(payload.points)) {
      await upsertDevicePoint(client, deviceId, pointKey, value, ts);
      await upsertPointLatest(client, deviceId, pointKey, value, ts, sourceTopic);
      await insertPointHistory(client, deviceId, pointKey, value, ts, sourceTopic);
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function handleEvent(payload, fallback) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await ensureDeviceExists(client, payload, fallback);
    await insertEventLog(client, payload, fallback);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

function telemetryToPointsPayload(payload, fallback) {
  const points = {};

  if (payload.t1 !== undefined) points.temp1 = payload.t1;
  if (payload.h1 !== undefined) points.hum1 = payload.h1;
  if (payload.t2 !== undefined) points.temp2 = payload.t2;
  if (payload.h2 !== undefined) points.hum2 = payload.h2;
  if (payload.t3 !== undefined) points.temp3 = payload.t3;
  if (payload.h3 !== undefined) points.hum3 = payload.h3;
  if (payload.t4 !== undefined) points.temp4 = payload.t4;
  if (payload.h4 !== undefined) points.hum4 = payload.h4;

  if (payload.fan1 !== undefined) points.output1 = payload.fan1;
  if (payload.fan2 !== undefined) points.output2 = payload.fan2;
  if (payload.out3 !== undefined) points.output3 = payload.out3;
  if (payload.out4 !== undefined) points.output4 = payload.out4;

  if (payload.dht1Ok !== undefined) points.dht1Ok = payload.dht1Ok;
  if (payload.dht2Ok !== undefined) points.dht2Ok = payload.dht2Ok;
  if (payload.dht3Ok !== undefined) points.dht3Ok = payload.dht3Ok;
  if (payload.dht4Ok !== undefined) points.dht4Ok = payload.dht4Ok;

  return {
    deviceId: payload.deviceId || fallback.deviceId,
    deviceName: payload.deviceName || null,
    site: payload.site || fallback.site,
    ts: payload.ts || new Date().toISOString(),
    points,
  };
}

async function routeMessage(topic, payload) {
  const info = topicParts(topic);

  if (info.root !== "9CC") {
    return;
  }

  if (!payload || typeof payload !== "object") {
    throw new Error("Payload is not valid JSON object");
  }

  switch (info.sub) {
    case "status":
      await handleStatus(payload, info);
      break;
    case "points":
      await handlePoints(payload, info, "points");
      break;
    case "event":
      await handleEvent(payload, info);
      break;
    case "telemetry": {
      const converted = telemetryToPointsPayload(payload, info);
      await handlePoints(converted, info, "telemetry");
      break;
    }
    default:
      console.log(`[MQTT] Ignored topic: ${topic}`);
  }
}

mqttClient.on("connect", async () => {
  console.log("[MQTT] Connected");

  for (const topic of TOPICS) {
    mqttClient.subscribe(topic, { qos: 0 }, (err) => {
      if (err) {
        console.error(`[MQTT] Subscribe fail: ${topic}`, err.message);
      } else {
        console.log(`[MQTT] Subscribed: ${topic}`);
      }
    });
  }

  try {
    const r = await pool.query("SELECT NOW() AS now");
    console.log("[PG] Connected:", r.rows[0].now);
  } catch (err) {
    console.error("[PG] Connection test failed:", err.message);
  }
});

mqttClient.on("reconnect", () => {
  console.log("[MQTT] Reconnecting...");
});

mqttClient.on("error", (err) => {
  console.error("[MQTT] Error:", err.message);
});

mqttClient.on("message", async (topic, messageBuffer) => {
  const text = messageBuffer.toString("utf8");
  const payload = safeJsonParse(text);

  if (!payload) {
    console.error(`[MQTT] Invalid JSON on ${topic}:`, text);
    return;
  }

  try {
    await routeMessage(topic, payload);
    console.log(`[OK] ${topic}`);
  } catch (err) {
    console.error(`[ERR] ${topic}:`, err.message);
  }
});

process.on("SIGINT", async () => {
  console.log("\n[APP] Shutting down...");
  try {
    mqttClient.end(true);
    await pool.end();
  } catch (_) {}
  process.exit(0);
});