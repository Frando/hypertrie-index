const varint = require('varint')
const { EventEmitter } = require('events')

module.exports = (...args) => new HypertrieIndex(...args)
module.exports.transformNode = transformNode

class Index extends EventEmitter {
  constructor (opts) {
    super()

    this._get = opts.get
    this._map = opts.map
    this._batchSize = opts.batchSize

    this._batch = []

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
  }

  _processBatch (msgs, cb) {
    if (!msgs.length) return cb()
    this._map(msgs, () => {
      this.emit('indexed', msgs)
      cb()
    })
  }

  run () {
    const self = this
    if (this._running) {
      this._continue = true
      return
    }

    this.emit('start')

    this._continue = false
    this._running = true

    this._fetchState((err, state) => {
      if (err) return this.emit('error', err)

      this._get(state, (err, results, workMoreCb) => {
        if (err) return this.emit('error', err)

        results = results || {}
        const { state, batch } = results

        if (!batch || !batch.length) return finish()

        this._batch = this._batch.concat(batch)

        if (this._batch.length < this._batchSize) return finish()

        const slice = this._batch.slice(0, this._batchSize)
        this._batch = this._batch.slice(this._batchSize)
        this._processBatch(slice, finish)

        function finish () {
          if (state) {
            self._storeState(state, (err) => {
              if (err) return self.emit('error', err)
              if (workMoreCb) workMoreCb(finalize)
            })
          } else finalize()
        }

        function finalize () {
          if (self._continue) {
            self._running = false
            self.run()
          } else if (self._batch.length) {
            const slice = self._batch.slice()
            self._batch = []
            self._processBatch(slice, close)
          } else close()
        }

        function close () {
          self._running = false
          self.emit('ready')
        }
      })
    })
  }
}

class HypertrieIndex extends EventEmitter {
  constructor (hypertrie, opts) {
    super()
    opts = opts || {}
    opts.batchSize = opts.batchSize || 100

    this._db = hypertrie
    this._opts = opts

    this._prefix = opts.prefix || ''
    // map, hidden, prefix, valueEncoding, transformNode

    this._index = new Index({
      storeState: opts.storeState,
      fetchState: opts.fetchState,
      clearIndex: opts.clearIndex,
      batchSize: opts.batchSize,
      get: this._get.bind(this),
      map: this._batch.bind(this)
    })

    this._index.on('indexed', batch => this.emit('indexed', batch))
    this._index.on('start', batch => this.emit('start'))
    this._index.on('ready', () => this.emit('ready'))

    this._db.ready(() => {
      this._watcher = this._db.watch(this._prefix)
      this._watcher.on('change', () => this._index.run())
      this._index.run()
    })
  }

  _currentSeq () {
    return this._db.feed.length
  }

  _get (stateBuf, cb) {
    const self = this
    const state = decodeState(stateBuf)

    if (!state.checkpoint && state.to === this._currentSeq()) {
      return cb()
    }

    let snapshot, from, to
    if (state.checkpoint) {
      snapshot = this._db.snapshot(state.to)
      from = state.from
      to = state.to
    } else {
      // Here, we catch all updates until now so reset the
      // continue marker.
      snapshot = this._db.snapshot()
      from = state.to
      to = snapshot.feed.length
    }

    const diff = snapshot.diff(from, this._prefix, {
      hidden: this._opts.hidden,
      checkpoint: state.checkpoint
    })

    let batch = []

    diff.next(map)

    function map (err, msg) {
      if (err) return finish(err)

      if (!msg) return finish(null, false)

      if (msg) batch.push(msg)

      if (batch.length === self._opts.batchSize) {
        finish(null, true)
      } else {
        diff.next(map)
      }
    }

    function finish (err, workLeft) {
      if (err) return cb(err)

      let checkpoint
      if (workLeft) {
        checkpoint = diff.checkpoint()
      }

      const state = encodeState({ from, to, checkpoint })
      cb(null, { state, batch }, (done) => {
        if (workLeft) {
          batch = []
          diff.next(map)
        } else done()
      })

      // cb(null, stateBuf, batch, workLeft)
    }
  }

  _batch (msgs, cb) {
    if (this._opts.transformNode) {
      msgs = msgs.map(msg => transformNode(msg, this._opts.valueEncoding))
    }
    this._opts.map(msgs, cb)
  }
}

function transformNode (node, valueEncoding) {
  let msg
  if (node.left) {
    msg = decodeValue(node.left, valueEncoding)
    msg.delete = false
    if (node.right) msg.previousNode = decodeValue(node.right)
  } else {
    msg = decodeValue(node.right, valueEncoding)
    msg.delete = true
  }
  return msg
}

function decodeValue (node, valueEncoding) {
  if (!valueEncoding) return node
  node.value = valueEncoding.decode(node.value)
  return node
}

function encodeState (state) {
  let { checkpoint, from, to } = state
  checkpoint = checkpoint || Buffer.alloc(0)
  from = from || 0
  to = to || 0
  let buf = Buffer.alloc(128)
  varint.encode(from, buf)
  let offset = varint.encode.bytes
  varint.encode(to, buf, offset)
  offset = offset + varint.encode.bytes
  let slice = buf.slice(0, offset)
  const final = Buffer.concat([slice, checkpoint])
  return final
}

function decodeState (buf) {
  if (!buf || !buf.length) return { from: 0 }
  let cur = buf
  let from = varint.decode(cur)
  cur = cur.slice(varint.decode.bytes)
  let to = varint.decode(cur)
  cur = cur.slice(varint.decode.bytes)
  let checkpoint = cur.length ? cur : null
  const state = { from, to, checkpoint }
  return state
}
