const tape = require('tape')
const hi = require('..')
const hypertrie = require('hypertrie')
const memdb = require('memdb')
const ram = require('random-access-memory')

tape('basics', t => {
  const feed = hypertrie(ram, { valueEncoding: 'json' })
  const lvl = memdb()
  const indexer = hi(feed, {
    map (msgs, done) {
      let missing = msgs.length
      for (let msg of msgs) {
        msg = hi.transformNode(msg)
        let key = `id:${msg.value.id}`
        if (msg.delete) lvl.del(key, finish)
        else lvl.put(key, msg.key, finish)
      }

      function finish () {
        if (--missing === 0) done()
      }
    }
  })

  indexer.on('ready', () => {
    // if (feed.length < 2) return
    let rs = lvl.createReadStream()
    collect(rs, (err, data) => {
      t.error(err, 'no error')
      t.equal(data.length, 2)
      t.equal(data[0].value, 'bla')
      t.equal(data[1].value, 'bar')
      t.end()
    })
  })

  feed.ready(() => {
    feed.put('foo', { id: 1 })
    feed.put('bar', { id: 2 })
    feed.put('bla', { id: 1 }, () => console.log(' last put'))
  })
})

function forAll (msgs, done, fn) {
  let missing = msgs.length
  for (let msg of msgs) {
    fn(msg, () => {
      if (--missing === 0) done()
    })
  }
}

tape('big', t => {
  const feed = hypertrie(ram, { valueEncoding: 'json' })
  const lvl = memdb()
  const indexer = hi(feed, {
    map (msgs, done) {
      // t.equal(msgs.length, 100, 'batch size correct')
      forAll(msgs, done, (msg, done) => {
        msg = hi.transformNode(msg)
        let key = `id:${('' + msg.value.id).padStart(4, 0)}`
        if (msg.delete) lvl.del(key, done)
        else lvl.put(key, msg.key, done)
      })
    }
  })

  let alldata = []

  let count = 0
  indexer.on('indexed', () => {
    count++
    console.log('index run', count)
  })

  indexer.on('ready', () => {
    finish()
  })

  feed.ready(() => {
    for (let i = 0; i < 1000; i++) {
      feed.put(`key-${i}`, { id: i })
    }
  })

  function finish () {
    let rs = lvl.createReadStream()
    collect(rs, (err, data) => {
      t.error(err, 'no error')
      alldata = [...alldata, ...data]
      t.equal(alldata.length, 1000, 'all correct')
      t.end()
    })
  }
})

tape('replication', t => {
  const t1 = hypertrie(ram, { valueEncoding: 'utf-8' })
  const lvl1 = memdb()
  const lvl2 = memdb()
  t1.ready(() => {
    const t2 = hypertrie(ram, t1.key, { valueEncoding: 'utf-8' })
    start(t1, t2)
  })
  function map (lvl) {
    return function (msgs, done) {
      forAll(msgs, done, (msg, done) => {
        msg = hi.transformNode(msg)
        if (msg.delete) lvl.del(msg.key, done)
        lvl.put(msg.value, msg.key, done)
      })
    }
  }
  function start (t1, t2) {
    const i1 = hi(t1, { map: map(lvl1) })
    const i2 = hi(t2, { map: map(lvl2) })
    i1.on('ready', () => collect(lvl1.createReadStream(), (err, data) => {
      t.error(err, 'no error')
      t.equal(data.length, 2)
      t.equal(data[1].key, 'moon')
    }))
    i2.on('ready', () => collect(lvl2.createReadStream(), (err, data) => {
      t.error(err, 'no error')
      t.equal(data.length, 2)
      t.equal(data[1].key, 'moon')
      t.end()
    }))
    t1.put('start', 'earth')
    t1.put('end', 'moon')
    replicate(t1, t2, () => {
      console.log('replication finished')
    })
  }
})

tape('prefix', t => {
  let missing = 0

  run(undefined, (err, results) => {
    t.error(err)
    console.log('undef', results)
    t.equal(results.length, 3, 'undef: results')
    finish()
  })
  run('take', (err, results) => {
    t.error(err)
    console.log('take', results)
    t.equal(results.length, 2, 'take: result')
    t.equal(results[0].value, 'hello', 'take: value matches')
    finish()
  })

  function finish () {
    if (--missing === 0) t.end()
  }

  function run (prefix, cb) {
    const feed = hypertrie(ram, { valueEncoding: 'utf8' })
    missing++
    let results = []
    const indexer = hi(feed, {
      prefix,
      transformNode: true,
      map (msgs, next) {
        results = [...results, ...msgs]
        next()
      }
    })

    indexer.on('ready', () => cb(null, results))

    feed.ready(() => {
      feed.put('take/bar', 'hello', (err) => {
        t.error(err)
        feed.put('not/foo', 'miss me', (err) => {
          t.error(err)
        })
        feed.put('take', 'hello', (err) => {
          t.error(err)
        })
      })
    })
  }
})

function replicate (a, b, cb) {
  var stream = a.replicate()
  stream.pipe(b.replicate()).pipe(stream).on('end', cb)
}

// function waitTicks (cnt) {
//   console.log('WAIT', cnt)
//   if (cnt === 0) return
//   process.nextTick(() => setTimeout(() => waitTicks(--cnt), 10))
// }

function collect (rs, cb) {
  let stack = []
  rs.on('data', d => stack.push(d))
  rs.on('end', () => cb(null, stack))
  rs.on('error', err => cb(err))
}
