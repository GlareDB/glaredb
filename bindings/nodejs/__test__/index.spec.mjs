import test from 'ava'

import glaredb from '../glaredb.js'

test('able to connect', async (t) => {
  t.notThrows(async () => {
    await glaredb.connect()
  })
})
