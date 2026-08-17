import assert from 'node:assert'
import { parseCardContent } from './cardContent.ts'

// Bullets
assert.deepStrictEqual(parseCardContent('- one\n- two'), [
  { type: 'bullet', text: 'one' },
  { type: 'bullet', text: 'two' }
])

// Plain text mixed with bullets
assert.deepStrictEqual(parseCardContent('intro\n- a\noutro'), [
  { type: 'text', text: 'intro' },
  { type: 'bullet', text: 'a' },
  { type: 'text', text: 'outro' }
])

// Closed fence
assert.deepStrictEqual(parseCardContent('```\nconst x = 1\n```'), [
  { type: 'code', text: 'const x = 1' }
])

// Unterminated fence runs to end
assert.deepStrictEqual(parseCardContent('before\n```\nlet y = 2\nlet z = 3'), [
  { type: 'text', text: 'before' },
  { type: 'code', text: 'let y = 2\nlet z = 3' }
])

// Fence content preserves internal whitespace/indentation
assert.deepStrictEqual(parseCardContent('```\n  indented\n```'), [
  { type: 'code', text: '  indented' }
])

console.log('cardContent.test.ts: all assertions passed')
