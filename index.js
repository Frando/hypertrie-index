const { EventEmitter } = require('events')

module.exports = (...args) => new HypertrieIndex(...args)
module.exports.transformNode = transformNode

class HypertrieIndex extends EventEmitter {
  constructor (hypertrie, opts) {
    super()
    opts = opts || {}
    this._db = hypertrie
    if (!opts.map) throw new Error('Map function is required.')
    this.mapfn = opts.map
    this._extended = !!(opts.extended)
    this._hidden = !!(opts.hidden)
    this._prefix = opts.prefix || ''
    this._transformNode = opts.transformNode
    this._valueEncoding = opts.valueEncoding

    if (!opts.storeState && !opts.fetchState && !opts.clearIndex) {
    // In-memory storage implementation
      let state
      this._storeState = function (buf, cb) {
        state = buf
        process.nextTick(cb)
      }
      this._fetchState = function (cb) {
        process.nextTick(cb, null, state)
      }
      this._clearIndex = function (cb) {
        state = null
        process.nextTick(cb)
      }
    } else {
      this._storeState = opts.storeState
      this._fetchState = opts.fetchState
      this._clearIndex = opts.clearIndex || null
    }

    this._db.ready(() => {
      this._watcher = this._db.watch(this._prefix)
      this._watcher.on('change', this.run.bind(this))
      this.run()
    })
  }

  run () {
    const self = this
    if (this._running) {
      this._continue = true
      return
    }

    this._running = true
    this._continue = false
    const snapshot = this._db.snapshot()

    this._fetchState((err, state) => {
      if (err) return close(err)
      snapshot.head((err, node) => {
        if (err || !node) return close(err)

        let from = 0
        if (state && state.from) from = state.from
        let to = node.seq + 1
        if (from >= to) return close()
        else process.nextTick(this._run.bind(this), snapshot, from, to, done)
      })
    })

    function done (err, seq) {
      if (err || !seq) return close(err)
      self._storeState({ from: seq }, err => {
        process.nextTick(close, err, seq)
      })
    }

    function close (err, seq) {
      if (err) self.emit('error', err)
      self._running = false
      if (self._continue) process.nextTick(self.run.bind(self))
      else if (seq) self.emit('ready')
    }
  }

  _run (db, from, to, cb) {
    console.log(db.feed.length, from, to)
    const self = this
    if (from >= to) return cb()
    const diff = db.diff(from, this._prefix, { hidden: this._hidden })

    // let batch = []

    diff.next(map)

    function map (err, node) {
      if (err) return cb(err)
      if (!node) return cb(null, to)
      let msg = node
      if (self._transformNode) msg = transformNode(msg)

      // if (batch.length >= self._batchSize) {
      //   self.mapfn(batch, () => {
      //     batch = []
      //     diff.next(map)
      //   })
      // } else {
      //   batch.push(msg)
      // }
      self.mapfn(msg, () => {
        diff.next(map)
      })
    }
  }
}

function transformNode (node, valueEncoding) {
  let msg
  if (node.left) {
    msg = decode(node.left)
    msg.delete = false
    if (node.right) msg.previousNode = decode(node.right)
  } else {
    msg = decode(node.right)
    msg.delete = true
  }
  return msg
  // let keys = ['key', 'value', 'delete', 'hidden']
  // return keys.reduce((agg, key) => {
  //   agg[key] = msg[key]
  //   return agg
  // }, {})

  function decode (node) {
    if (!valueEncoding) return node
    node.value = valueEncoding.decode(node.value)
    return node
  }
}
