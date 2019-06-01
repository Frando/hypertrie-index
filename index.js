const { EventEmitter } = require('events')

module.exports = (...args) => new HypertrieIndex(...args)

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
      this._watcher = this._db.watch('')
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
      if (err) return self.emit('error', err)
      let at
      if (!state) at = 0
      else at = state.at

      snapshot.head((err, node) => {
        if (err || !node) return close(err)
        let to = node.seq + 1
        if (at >= to) return close()
        else process.nextTick(this._run.bind(this), snapshot, at, to, done)
      })
    })

    function done (err, seq) {
      if (err || !seq) return close(err)
      self._storeState({ at: seq }, err => {
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

  _run (db, at, to, cb) {
    const self = this
    if (at >= to) return cb()
    const diff = db.diff(at, this._prefix, { hidden: this._hidden })
    diff.next(map)

    function map (err, node) {
      if (err) return cb(err)
      if (!node) return cb(null, to)
      let msg = node
      if (!self._extended) msg = simplify(node)
      self.mapfn(msg, () => {
        diff.next(map)
      })
    }

    function simplify (node) {
      let msg
      if (node.left) {
        msg = node.left
        msg.delete = false
      } else {
        msg = { ...node.right }
        msg.delete = true
      }
      let keys = ['key', 'value', 'delete', 'hidden']
      return keys.reduce((agg, key) => {
        agg[key] = msg[key]
        return agg
      }, {})
    }
  }
}
