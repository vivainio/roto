import mqtt from 'mqtt';
import { test } from 'bun:test';
import { resolve } from 'node:path';

const root = resolve(import.meta.dir, '../..');
let port;

const log = console.log;
const status = console.log;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const clients = new Set();
async function config() { return { mqttUrl: `ws://127.0.0.1:${port}/mqtt`, hooks: true }; }
function within(promise, label) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(label + ' timed out')), 5000);
    promise.then(value => { clearTimeout(timeout); resolve(value); }, error => { clearTimeout(timeout); reject(error); });
  });
}
function event(client, name, predicate = () => true) {
  return within(new Promise((resolve, reject) => {
    const handler = (...args) => { if (predicate(...args)) { cleanup(); resolve(args); } };
    const error = reason => { cleanup(); reject(reason); };
    const cleanup = () => { client.off(name, handler); client.off('error', error); };
    client.on(name, handler); client.once('error', error);
  }), name);
}
async function open(clientId = 'demo-' + Math.random().toString(16).slice(2)) {
  const { mqttUrl } = await config();
  const c = mqtt.connect(mqttUrl, {
    // Bun does not implement ws.createWebSocketStream; use its native client.
    forceNativeWebSocket: true, protocolVersion: 4, clientId, clean: false, keepalive: 1, reconnectPeriod: 0,
    username: 'fake', password: 'fake', will: { topic: 'demo/will', payload: 'ignored', qos: 1 },
  });
  clients.add(c);
  c.on('error', error => log('ERROR: ' + error.message));
  try { await event(c, 'connect'); return c; }
  catch (error) { await c.endAsync(true); clients.delete(c); throw error; }
}
async function publish(topic, payload) {
  const response = await fetch(`http://127.0.0.1:${port}/topics/` + encodeURIComponent(topic) + '?qos=1', { method: 'POST', body: payload, signal: AbortSignal.timeout(5000), headers: { Host: `iotdata.localhost:${port}` } });
  if (!response.ok) throw new Error(await response.text());
}
function check(condition, message) { if (!condition) throw new Error(message); log('PASS: ' + message); }
async function checks() {
  status('Running acceptance checks…');
  try {
    const first = await open('acceptance');
    const second = await open('acceptance-second');
    const count = [];
    first.on('message', (...args) => count.push(args));
    check(first.stream.socket.protocol === 'mqtt', 'WebSocket mqtt subprotocol');
    const grants = await within(first.subscribeAsync({ 'demo/state': { qos: 1 }, '$aws/things/ping': { qos: 0 } }), 'subscribe');
    check(grants[0].qos === 1 && grants[1].qos === 0, 'QoS 0/1 subscription grants');
    await within(second.subscribeAsync('demo/+', { qos: 1 }), 'wildcard subscribe');
    const delivered = event(first, 'message');
    const wildcard = event(second, 'message');
    const binary = new Uint8Array(20000).fill(255);
    await publish('demo/state', binary);
    const [topic, payload, packet] = await delivered;
    check(topic === 'demo/state' && payload.length === binary.length && payload.every(byte => byte === 255)
      && packet.qos === 0, 'Binary HTTP publish delivered at QoS 0, multi-byte packet length');
    check((await wildcard)[1].length === binary.length, 'Shared registry wildcard delivery');
    const health = event(first, 'message');
    await publish('$aws/things/ping', 'healthy');
    check((await health)[1].toString() === 'healthy', '$aws/things/ping delivery');
    const before = count.length;
    await within(first.publishAsync('demo/state', 'dropped', { qos: 1 }), 'PUBACK');
    await sleep(100);
    check(count.length === before, 'Client QoS 1 publish acknowledged and dropped');
    await within(first.unsubscribeAsync('demo/state'), 'unsubscribe');
    const remaining = event(second, 'message');
    await publish('demo/state', 'after unsubscribe');
    await remaining; await sleep(100);
    check(count.length === before, 'Unsubscribe stops delivery');
    await event(first, 'packetreceive', packet => packet.cmd === 'pingresp');
    check(first.connected, 'MQTT keepalive PINGRESP');
    await first.subscribeAsync('demo/state', { qos: 1 });
    await first.endAsync();
    const fresh = await open('acceptance');
    const freshMessages = [];
    fresh.on('message', (...args) => freshMessages.push(args));
    const existing = event(second, 'message');
    await publish('demo/state', 'no old session'); await existing; await sleep(100);
    check(freshMessages.length === 0, 'Reconnect has no saved subscriptions');
    await fresh.subscribeAsync('demo/state', { qos: 1 });
    const resubscribed = event(fresh, 'message');
    await publish('demo/state', 'fresh');
    check((await resubscribed)[1].toString() === 'fresh', 'Resubscribe after reconnect');
    const { hooks } = await config();
    if (hooks) {
      for (const [id, code] of [['denied', 5], ['hook-error', 3], ['invalid-hook', 3]]) {
        let denied;
        try { await open(id); }
        catch (error) { denied = error; }
        check(denied && denied.code === code, 'Lua connect refusal ' + id + ' (code ' + code + ')');
      }
      const suback = event(fresh, 'packetreceive', packet => packet.cmd === 'suback');
      await new Promise(resolve => fresh.subscribe({
        'demo/state': { qos: 1 }, 'demo/private/#': { qos: 1 },
      }, () => resolve()));
      const [ack] = await suback;
      check(ack.granted[0] === 1 && ack.granted[1] === 128, 'Lua per-filter subscribe refusal');
      await second.subscribeAsync('demo/#', { qos: 1 });
      const observed = event(second, 'message');
      const beforeDenied = freshMessages.length;
      await publish('demo/private/item', 'denied filter'); await observed; await sleep(100);
      check(freshMessages.length === beforeDenied, 'Denied filter does not receive publishes');
      const allowed = event(fresh, 'message');
      await publish('demo/state', 'allowed filter');
      check((await allowed)[1].toString() === 'allowed filter', 'Allowed filter still receives publishes');
    }
    status('PASS: all MQTT.js browser acceptance checks' + (hooks ? ' including Lua denials' : ''));
  } finally { for (const c of clients) { await within(c.endAsync(true), 'disconnect'); clients.delete(c); } }
}

test('MQTT.js over WebSocket, including Lua policy denials', async () => {
  // Reserve a free port alongside any existing development server.
  const reservation = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Response() });
  port = reservation.port;
  reservation.stop(true);
  const server = Bun.spawn([
    process.env.ROTO_SERVER || resolve(root, 'target/debug/roto-server'),
    '--ephemeral', '--port', String(port), '--setup', 'demos/mqtt/setup.lua',
  ], { cwd: root, stdout: 'inherit', stderr: 'inherit' });
  try {
    const deadline = Date.now() + 10000;
    while (true) {
      if (server.exitCode !== null) throw new Error(`Roto exited with ${server.exitCode}`);
      try {
        const response = await fetch(`http://127.0.0.1:${port}/roto-api/health`, {
          signal: AbortSignal.timeout(500),
        });
        if (response.ok) break;
      } catch {}
      if (Date.now() >= deadline) throw new Error('Roto did not become ready');
      await sleep(50);
    }
    await checks();
  } finally {
    server.kill();
    await server.exited;
  }
}, 30000);

