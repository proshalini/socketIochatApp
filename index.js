import express from "express";
import { createServer } from "node:http";
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { Server } from "socket.io";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import { availableParallelism } from "node:os";
import cluster from "node:cluster";
import { createAdapter, setupPrimary } from "@socket.io/cluster-adapter";

//open the database file
if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i
    });
  }

  setupPrimary();
} else {
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

//create the message table 
await db.exec(`CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_offset TEXT UNIQUE,
  message TEXT,
  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
)`);


const app = express();
const server = createServer(app);
const io = new Server(server, {
  connectionStateRecovery: {},
  //set up adapter on each working thread
  adapter: createAdapter()
});

const __dirname = dirname(fileURLToPath(import.meta.url));
app.get('/', (req, res) => {
  res.sendFile(join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
  socket.on('chat message', async (msg,clientOffset,callback) => {
    let result;
    try {
      //store message in database
      result = await db.run(`INSERT INTO messages(message) VALUES (?)`, msg);
    }
    catch (e) {
      if(e.errno==19){
        //sqlite constraint
        callback();
      }
      else{
        //nothing to do let the client retry
      }
      return;
      //handle 
    }
    //also include offset with msg
    io.emit('chat message', msg, result.lastID);
  })
  if (!socket.recovered) {
    //if connection state recovery was not successful
    try {
      db.each(`SELECT id,message FROM messages Where id>(?)`, [socket.handshake.auth.serverOffset || 0],
        (_err, row) => {
          socket.emit('chat message', row.message, row.id);
        }
      )
    } catch (e) {
      console.log("something went wrong while recovering", e.message);
    }
  };
});

const port = process.env.PORT;

server.listen(port, () => {
  console.log(`server is running on port ${port}`);
});
}