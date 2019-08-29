const varint = require('varint')
const codecs = require('codecs')
const { EventEmitter } = require('events')

module.exports = hypertrieIndex
module.exports.transformNode = transformNode

function hypertrieIndex (db, opts, cb) {
  cb = cb || noop
  const emitter = new EventEmitter()
  // const { live, prefix, batchSize, map, fetchState, storeState } = opts

  let batch, diff, next, snapshot

  let state = opts.state || {}
  let init = false
  let paused = false
  let running = false

  opts.live = defaultTrue(opts.live)
  opts.valueEncoding = opts.valueEncoding ? codecs(opts.valueEncoding) : db.valueEncoding

  if (opts.fetchState) {
    opts.fetchState((err, fetchedState) => {
      if (err) return cb(err)
      state = decodeState(fetchedState)
      run()
    })
  } else run()

  emitter.pause = function () {
    paused = true
    emitter.emit('pause')
  }
  emitter.resume = function () {
    paused = false
    if (!init) return
    if (!running) {
      emitter.emit('resume')
      run()
    }
  }
  emitter.isRunning = () => running

  return emitter

  function run () {
    if (running) return
    // Skip the header entry.
    // console.log('RUN', state.seq, state.checkpoint ? true : null)
    if (!init) {
      init = true
      if (opts.live) db.watch(opts.prefix || '', run)
    }
    if (db.version < 2) return
    running = true

    emitter.emit('start', state)

    const { seq, checkpoint } = state

    if (checkpoint && checkpoint.length) {
      snapshot = db.snapshot(seq)
      diff = snapshot.diff(checkpoint, opts.prefix)
    } else {
      snapshot = db.snapshot()
      diff = snapshot.diff(seq, opts.prefix)
    }

    next = diff.next.bind(diff, _next)
    batch = []

    if (!paused) {
      next()
    } else {
      running = false
    }
  }

  function _next (err, msg) {
    if (err) emitter.emit('error', err)
    if (msg) {
      batch.push(msg)
      if (paused) {
        forward(finish)
      } else if (batch.length < opts.batchSize) {
        next()
      } else {
        forward(next)
      }
    } else {
      forward(finish)
    }
  }

  function forward (cb) {
    if (!batch.length) return updateState(cb, [])

    batch = batch.map(msg => decodeValues(msg, opts.valueEncoding))
    if (opts.transformNode) batch = batch.map(transformNode)

    opts.map(batch, () => {
      updateState(cb, batch)
      batch = []
    })
  }

  function updateState (done, batch) {
    state = {
      seq: snapshot.version,
      checkpoint: diff.checkpoint()
    }
    emitter.emit('state', state)
    if (opts.storeState) {
      opts.storeState(encodeState(state), (err) => {
        let complete = state.seq === db.version && !state.checkpoint.length
        if (batch.length) emitter.emit('indexed', batch, complete)
        if (err) return cb(err)
        done()
      })
    } else done()
  }

  function finish () {
    running = false
    if (paused && opts.live) return
    if (db.version > snapshot.version && opts.live) {
      process.nextTick(run)
    } else {
      emitter.emit('finished')
      emitter.emit('ready')
      cb()
    }
  }
}

function encodeState (state) {
  let { checkpoint, seq } = state

  checkpoint = checkpoint || Buffer.alloc(0)
  seq = seq || 0

  let buf = Buffer.alloc(128)
  varint.encode(seq, buf)
  let offset = varint.encode.bytes
  let slice = buf.slice(0, offset)

  const final = Buffer.concat([slice, checkpoint])
  return final
}

function decodeState (buf) {
  if (!buf || !buf.length) return {}
  let cur = buf
  let seq = varint.decode(cur)
  cur = cur.slice(varint.decode.bytes)
  let checkpoint = cur.length ? cur : null
  const state = { seq, checkpoint }
  return state
}

function transformNode (node) {
  let msg
  if (node.left) {
    msg = node.left
    msg.delete = false
    if (node.right) msg.previousNode = msg.right
  } else {
    msg = node.right
    msg.delete = true
  }
  return msg
}

function decodeValue (node, valueEncoding) {
  if (!valueEncoding) return node
  if (!Buffer.isBuffer(node.value)) return node
  node.value = valueEncoding.decode(node.value)
  return node
}

function decodeValues (msg, valueEncoding) {
  if (msg.left) msg.left = decodeValue(msg.left, valueEncoding)
  if (msg.right) msg.right = decodeValue(msg.right, valueEncoding)
  return msg
}

function noop () {}

function defaultTrue (val) {
  return typeof val === 'undefined' ? true : val
}
