export type CardContentBlock =
  { type: 'bullet'; text: string } | { type: 'code'; text: string } | { type: 'text'; text: string }

/**
 * Markdown-lite parser for card front/back text: `- ` bullets and ``` fenced
 * code blocks. Everything else passes through as plain lines.
 */
export function parseCardContent(text: string): CardContentBlock[] {
  const lines = text.split('\n')
  const blocks: CardContentBlock[] = []
  let i = 0

  while (i < lines.length) {
    const line = lines[i]

    if (line === '```') {
      const codeLines: string[] = []
      i++
      while (i < lines.length && lines[i] !== '```') {
        codeLines.push(lines[i])
        i++
      }
      if (i < lines.length) i++ // consume closing fence
      blocks.push({ type: 'code', text: codeLines.join('\n') })
      continue
    }

    if (line.startsWith('- ')) {
      blocks.push({ type: 'bullet', text: line.slice(2) })
      i++
      continue
    }

    blocks.push({ type: 'text', text: line })
    i++
  }

  return blocks
}
