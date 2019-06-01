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
  feed.put('id1', { type: 'planet', name: 'earth' })
  feed.put('id2', { type: 'river', name: 'nile' })
  feed.put('id3', { type: 'planet', name: 'marsss' })
  feed.put('id4', { type: 'planet', name: 'venus' })
  feed.put('id3', { type: 'planet', name: 'mars' })
  feed.del('id3')
})
