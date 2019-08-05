# hypertrie-index

Run an indexing function over a hypertrie.

## API

```js
const hypertrieIndex = require('hypertrie-index')
```

#### `const indexer = hypertrieIndex(hypertrie, opts)`

`hypertrie` is a [hypertrie](https://github.com/mafintosh/hypertrie) instance.
`opts` is an object of options (with their defaults):

* `map`: The map function to run on each batch.
* `batchSize`: Max number of entries to pass into the map function.
* `storeState: function (buf, cb)`, `fetchState: function (cb)`: Function to store and retrieve the current state. `buf` is a buffer that should be stored as-is in some storage backend, and be passed back into `cb(null, buf)` in `fetchState`.
* `live: true` Keep watching for changes
* `prefix: ''`: Set a prefix to only index and watch a part of the trie.
* `transformNode: false`: Pass all messages through `hypertrie.transformNode`

The map function will receive an array of change messages. The messages are in the format returned from `hypertrie.diff`: `{ left: { key, value, seq }, right: { key, value, seq } }`. If passed through `hypertrie.transformNode`, they are in the format `{ key, value, seq, deleted, previousValue }` where `deleted` is false for puts and true for deletes. In case of updates `previousValue` will contain the previous value.

#### `indexer.pause()`

Pause the indexing.

#### `indexer.resume()`

Resume the indexing.

#### `indexer.on('indexed', function (batch) {}`

Emitted after each round of indexing. `batch` is the array of change messages.

#### `indexer.on('finished', function () {})`

Emitted when indexing finishes.

