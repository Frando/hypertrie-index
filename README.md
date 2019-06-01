# hypertrie-index

Run an indexing function over a hypertrie.

## Usage

```js
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
```

## API

#### `const indexer = hypertrieIndex(feed, [opts])`

`feed`: A hypertrie feed

`opts`: An opts object with a required `map` function (see below)..

* `opts` can include: `hidden` to work on the hidden namespace, `storeState`, `fetchState` to optionally store the processing state (currently expects JSON)

The returned `indexer` emits a `ready` object whenever a round of processing is finished. It automatically listens on changes of the hypertrie.

The `map` function is invoked for each change. It gets an object of the form `{ key, value, deleted }` where deleted is true when a value was deleted. Optionally, set the `extended`option in the constructor to instead get the full change object (as is returned by hypertrie's diff iterator).

*(to be continued)*
