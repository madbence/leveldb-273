'use strict';
const uuid = require('uuid');
let id = uuid.v4();
let db = require('levelup')(`./db-${id}`);

const val = 'a'.repeat(2000);

function write(n) {
  const s = Date.now();
  function put(m) {
    if (!m) {
      console.log('Appending %d keys took %dms', n, Date.now() - s);
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      db.put(Date.now() + '|' + uuid.v4(), val, err => {
        if (err) return reject(err);
        resolve();
      })
    }).then(() => {
      return put(m - 1);
    });
  }
  return put(n);
}

function proc() {
  const s = Date.now();
  let count = 0;
  const keys = [];
  return new Promise((resolve, reject) => {
    db.createReadStream()
      .on('data', v => {
        keys.push(v.key);
        count++;
      })
      .on('end', () => {
        function del() {
          return new Promise(resolve => {
            let db_ = db;
            let id_ = id;
            id = uuid.v4();
            db = require('levelup')(`./db-${id}`);
            resolve();
            db_.close(err => {
              require('leveldown').destroy(`./db-${id_}`, err => {
              });
            });
          });
        }
        del().then(() => {
          db.db.approximateSize('', 'z', (err, size) => {
            if (err) return reject(err);
            console.log('Processing %d keys took %dms (db size after proc: %dMB)', count, Date.now() - s, (size / 1024 / 1024).toFixed(2));
            resolve();
          });
        });
      });
  });
}

function sleep(s) {
  return () => new Promise(resolve => setTimeout(resolve, s * 1000));
}

function loop() {
  return write(10000).then(proc).then(proc).then(loop);
}

loop().catch(err => console.error(err.stack));
