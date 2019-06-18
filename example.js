const hypertrie = require('hypertrie')
const ram = require('random-access-memory')
const hypertrieIndex = require('.')
const memdb = require('memdb')

const feed = hypertrie(ram, { valueEncoding: 'json' })
const lvl = memdb()

const indexer = hypertrieIndex(feed, {
  map (msg, done) {
    let key = `t:${msg.value.type}:${msg.value.name}`
    if (msg.delete) {
      lvl.del(key, done)
    } else {
      lvl.put(key, msg.key, done)
    }
  }
})

indexer.on('ready', () => {
  let rs = lvl.createReadStream()
  rs.on('data', console.log)
})

feed.ready(() => {
  feed.put('earth', { type: 'planet', name: 'earth' })
  feed.put('nile', { type: 'river', name: 'nile' })
  feed.put('mars', { type: 'planet', name: 'marsss' })
  feed.put('venus', { type: 'planet', name: 'venus' })
  feed.put('mars', { type: 'planet', name: 'mars' })
  feed.del('venus')
  feed.put('foo', { type: 'planet', name: 'mars' })
})

let history = feed.createHistoryStream({ live: true })
history.on('data', d => console.log('history', d))
