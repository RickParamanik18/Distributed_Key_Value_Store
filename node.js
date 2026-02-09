const express = require("express");
const axios = require("axios");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 3001);
const NODE_ID = process.env.NODE_ID || `node-${PORT}`;

const nodeUrls = (
    process.env.NODES ||
    "http://localhost:3001,http://localhost:3002,http://localhost:3003,http://localhost:3004,http://localhost:3005,http://localhost:3006"
).split(",");

const RF = 3,
    W = 2,
    R = 2;

const selfUrl = `http://localhost:${PORT}`;

const dataDirectory = "./data";
if (!fs.existsSync(dataDirectory)) fs.mkdirSync(dataDirectory);

const walFile = path.join(dataDirectory, `wal-${PORT}.log`);
const snapshotFile = path.join(dataDirectory, `snapshot-${PORT}.json`);

function hashToInt(v) {
    const hash = crypto.createHash("sha1").update(v).digest("hex").slice(0, 8);
    return parseInt(hash, 16);
}

class ConsistentHashRing {
    constructor(vNodes = 5) {
        this.vNodes = vNodes;
        this.ring = new Map();
        this.sorted = [];
    }

    build(nodes) {
        this.ring.clear();
        this.sorted = [];
        for (const node of nodes) {
            for (let i = 0; i < this.vNodes; i++) {
                const hash = hashToInt(`${node}-${i}`);
                this.ring.set(hash, node);
                this.sorted.push(hash);
            }
        }
        this.sorted.sort((a, b) => a - b);
    }

    owners(key, rf = RF) {
        const hashedKey = hashToInt(key);
        const res = [];
        if (!this.sorted.length) return res;

        let i = this.sorted.findIndex((hash) => hashedKey <= hash);
        if (i === -1) i = 0;

        const seen = new Set();
        while (res.length < rf) {
            const node = this.ring.get(this.sorted[i]);
            if (!seen.has(node)) {
                seen.add(node);
                res.push(node);
            }
            i = (i + 1) % this.sorted.length;
        }
        return res;
    }
}

const ring = new ConsistentHashRing(5);
ring.build(nodeUrls);

const store = new Map();

function getTimeStamp() {
    return Date.now();
}

function appendWAL(entry) {
    fs.appendFileSync(walFile, JSON.stringify(entry) + "\n");
}

function applyEntry(entry) {
    const cur = store.get(entry.key);
    if (!cur || entry.ts >= cur.ts) {
        store.set(entry.key, { value: entry.value, ts: entry.ts });
    }
}

function createSnapshot() {
    const snapshot = {
        data: Array.from(store.entries()),
        createdAt: Date.now(),
    };

    fs.writeFileSync(snapshotFile, JSON.stringify(snapshot));
    if (fs.existsSync(walFile)) {
        fs.truncateSync(walFile, 0);
    }

    console.log("Snapshot created");
}

// taking snapshot after every 30 seconds
setInterval(createSnapshot, 30000);

// ---------- RECOVERY ----------
function recover() {
    // Loading snapshot
    if (fs.existsSync(snapshotFile)) {
        const snap = JSON.parse(fs.readFileSync(snapshotFile));
        for (const [k, v] of snap.data) {
            store.set(k, v);
        }
        console.log("Snapshot loaded");
    }

    // Replaying WAL
    if (fs.existsSync(walFile)) {
        const lines = fs.readFileSync(walFile, "utf-8").trim().split("\n");
        for (const line of lines) {
            if (!line) continue;
            const entry = JSON.parse(line);
            applyEntry(entry);
        }
        console.log("WAL replayed");
    }
}

recover();

const app = express();
app.use(express.json());

// Health
app.get("/health", (req, res) => res.json({ success: true, node: NODE_ID }));

// Not Exposing , for Internal Replication
app.post("/internal/put", (req, res) => {
    const { key, value, ts } = req.body;

    const entry = { key, value, ts };

    appendWAL(entry);
    applyEntry(entry);

    res.json({ success: true });
});

app.get("/internal/get/:key", (req, res) => {
    res.json(store.get(req.params.key) || null);
});

//Exposing for Users
app.put("/store/:key", async (req, res) => {
    const key = req.params.key;
    const value = req.body.value;
    const ts = getTimeStamp();

    const owners = ring.owners(key);
    let acks = 0;

    await Promise.all(
        owners.map(async (u) => {
            try {
                await axios.post(
                    `${u}/internal/put`,
                    { key, value, ts },
                    { timeout: 800 },
                );
                acks++;
            } catch {}
        }),
    );

    if (acks >= W) return res.json({ success: true, acks });
    return res.status(503).json({ success: false, acks });
});

app.get("/store/:key", async (req, res) => {
    const key = req.params.key;
    const owners = ring.owners(key);

    const replies = [];

    await Promise.all(
        owners.map(async (u) => {
            try {
                const r = await axios.get(`${u}/internal/get/${key}`, {
                    timeout: 800,
                });
                if (r.data) replies.push(r.data);
            } catch {}
        }),
    );

    if (replies.length < R) return res.status(503).json({ success: false });

    replies.sort((a, b) => b.ts - a.ts);
    const latest = replies[0];

    owners.forEach(async (u) => {
        try {
            await axios.post(`${u}/internal/put`, {
                key,
                value: latest.value,
                ts: latest.ts,
            });
        } catch {}
    });

    res.json({ success: true, value: latest.value });
});

app.listen(PORT, () => {
    console.log(`Node ${NODE_ID} running at ${selfUrl}`);
});
