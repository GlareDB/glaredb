import test from 'ava'

import glaredb from '../index.js'

test('able to connect', async (t) => {
  await glaredb.connect()
})
