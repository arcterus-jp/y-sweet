import { expect, test } from 'vitest'

test('Can import SDK via require', () => {
  const sdk = require('@arcterus-jp/sdk')

  expect(sdk).toBeDefined()
  expect(sdk.DocumentManager).toBeDefined()
})

test('Can import React library via require', () => {
  const react = require('@arcterus-jp/react')

  expect(react).toBeDefined()
  expect(react.useYDoc).toBeDefined()
})

test('Can import client library via require', () => {
  const client = require('@arcterus-jp/client')

  expect(client).toBeDefined()
  expect(client.createYjsProvider).toBeDefined()
})
