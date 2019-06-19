const varint = require('varint')
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
    this._batchSize = opts.batchSize || 100

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
    if (this._running) {
      this._continue = true
      return
    }

    this._running = true
    this._work()
  }

  _work () {
    const self = this

    // Default state.
    let state = { from: 0 }
    let batch = []

    self._fetchState((err, buf) => {
      if (err) return finalize(err)
      if (buf) state = decodeState(buf)

      // now start a diff with this state info
      process.nextTick(start, state)
    })

    function start () {
      self._continue = false
      if (state.from >= state.to) finish(null, state, batch)
      let snapshot
      if (state.checkpoint) {
        snapshot = self._db.snapshot(state.to)
      } else {
        // Here, we catch all updates until now so reset the
        // continue marker.
        snapshot = self._db.snapshot()
        state.from = state.to
        state.to = snapshot.feed.length
      }

      const diff = snapshot.diff(state.from, self._prefix, {
        hidden: self._hidden,
        checkpoint: state.checkpoint
      })

      iterate(diff)
    }

    function iterate (diff) {
      diff.next(map)

      function map (err, msg) {
        if (err) return finish(err, state, batch)
        if (msg) batch.push(msg)

        if (!msg) {
          state.checkpoint = null
          finish(null, state, batch)
        } else if (batch.length === self._batchSize) {
          state.checkpoint = diff.checkpoint()
          finish(null, state, batch)
        } else {
          diff.next(map)
        }
      }
    }

    function finish (err) {
      if (err || !batch || state.from === state.to) return finalize(err, state)
      // Batch size is not yet full and there's updates.
      if (batch.length < self._batchSize && self._continue) {
        return start()
      }
      if (batch.length < self._batchSize && !state.checkpoint && self._db.feed.length > state.to) {
        return start()
      }

      if (batch.length) self.mapfn(batch, () => finalize(null, state, batch))
      else finalize(null, state)
    }

    function finalize (err, state, batch) {
      if (!state || err) {
        if (err) self.emit('error', err)
        self._running = false
        return
      }

      self._storeState(encodeState(state), err => {
        if (err) self.emit('error', err)
        self._running = false
        if (batch && batch.length) self.emit('ready')
        if (self._continue || state.checkpoint) self.run()
      })
    }
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
