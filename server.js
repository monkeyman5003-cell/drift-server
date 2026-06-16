// ============================================================================
//  Drift City — multiplayer relay server
//  A tiny WebSocket server that relays player state between everyone in a room.
//  It does NOT simulate the game; it just forwards each player's position to
//  the others as fast as it arrives. That's what makes movement feel instant.
// ============================================================================

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

// Create a basic HTTP server (Render needs something listening on the port,
// and it's handy for a health check in the browser).
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Drift City server is running.\n');
});

const wss = new WebSocketServer({ server });

// rooms: Map<roomName, Map<playerId, ws>>
const rooms = new Map();
// also remember each player's last known state so a newcomer sees others immediately
const lastState = new Map(); // Map<roomName, Map<playerId, stateObj>>

function getRoom(name) {
  if (!rooms.has(name)) rooms.set(name, new Map());
  if (!lastState.has(name)) lastState.set(name, new Map());
  return rooms.get(name);
}

function broadcast(roomName, senderId, dataObj) {
  const room = rooms.get(roomName);
  if (!room) return;
  const msg = JSON.stringify(dataObj);
  for (const [pid, client] of room) {
    if (pid !== senderId && client.readyState === 1) {
      client.send(msg);
    }
  }
}

wss.on('connection', (ws) => {
  ws.playerId = null;
  ws.roomName = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // --- join a room ---
    if (msg.type === 'join') {
      const roomName = (msg.room || 'default').slice(0, 40);
      const room = getRoom(roomName);

      // limit to 2 players per room for now
      if (room.size >= 2 && !room.has(msg.id)) {
        ws.send(JSON.stringify({ type: 'full' }));
        return;
      }

      ws.playerId = msg.id;
      ws.roomName = roomName;
      room.set(msg.id, ws);

      // assign a slot (1 or 2) based on who's already here
      const usedSlots = [];
      for (const [pid, c] of room) { if (c.slot) usedSlots.push(c.slot); }
      ws.slot = usedSlots.includes(1) ? 2 : 1;

      // tell the joiner their slot and how many are in the room
      ws.send(JSON.stringify({ type: 'joined', slot: ws.slot, count: room.size }));

      // send the joiner the last known state of everyone already here
      const states = lastState.get(roomName);
      for (const [pid, st] of states) {
        if (pid !== msg.id) ws.send(JSON.stringify({ type: 'state', id: pid, slot: st.slot, state: st.state }));
      }

      // tell everyone (incl. joiner) the new room count + that someone joined
      const room2 = rooms.get(roomName);
      for (const [pid, client] of room2) {
        if (client.readyState === 1) {
          client.send(JSON.stringify({ type: 'roomcount', count: room2.size }));
        }
      }
      return;
    }

    // --- a player streaming their state ---
    if (msg.type === 'state' && ws.roomName && ws.playerId) {
      const states = lastState.get(ws.roomName);
      if (states) states.set(ws.playerId, { slot: ws.slot, state: msg.state });
      broadcast(ws.roomName, ws.playerId, {
        type: 'state', id: ws.playerId, slot: ws.slot, state: msg.state
      });
      return;
    }
  });

  ws.on('close', () => {
    if (ws.roomName && rooms.has(ws.roomName)) {
      const room = rooms.get(ws.roomName);
      room.delete(ws.playerId);
      const states = lastState.get(ws.roomName);
      if (states) states.delete(ws.playerId);

      // tell remaining players someone left + new count
      for (const [pid, client] of room) {
        if (client.readyState === 1) {
          client.send(JSON.stringify({ type: 'left', id: ws.playerId }));
          client.send(JSON.stringify({ type: 'roomcount', count: room.size }));
        }
      }
      if (room.size === 0) { rooms.delete(ws.roomName); lastState.delete(ws.roomName); }
    }
  });
});

server.listen(PORT, () => {
  console.log('Drift City server listening on port ' + PORT);
});
