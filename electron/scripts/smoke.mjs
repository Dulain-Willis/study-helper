import { _electron as electron } from 'playwright-core'

const app = await electron.launch({
  args: ['--user-data-dir=/tmp/study-helper-smoke', '.'],
  cwd: process.cwd()
})
const page = await app.firstWindow()
await page.waitForTimeout(1500)

await page.click('text=+ New Group')
await page.waitForTimeout(400)
await page.keyboard.press('Enter')
await page.waitForTimeout(400)

await page.hover('.node-row')
await page.click('text=+ Set')
await page.waitForTimeout(400)
await page.keyboard.press('Enter')
await page.waitForTimeout(400)
await page.screenshot({ path: '/tmp/shots-01-set-created.png' })

// open the set
await page.click('.node-name-clickable')
await page.waitForTimeout(500)
await page.screenshot({ path: '/tmp/shots-02-cardlist-empty.png' })

// add a card
await page.fill('input[placeholder="Front"]', 'What is 2+2?')
await page.fill('input[placeholder="Back"]', '4')
await page.click('text=+ Add Card')
await page.waitForTimeout(400)
await page.screenshot({ path: '/tmp/shots-03-card-added.png' })

let html = await page.content()
console.log('card shown after add:', html.includes('What is 2+2?') && html.includes('>4<'))

// edit the card
await page.click('text=Edit')
await page.waitForTimeout(300)
const inputs = await page.$$('.card-row input')
await inputs[0].fill('What is 3+3?')
await inputs[1].fill('6')
await page.click('button[aria-label="Confirm"]')
await page.waitForTimeout(400)
html = await page.content()
console.log('card updated:', html.includes('What is 3+3?') && html.includes('>6<'))

// delete the card, auto-confirm the native dialog
page.once('dialog', (d) => d.accept())
await page.click('text=Delete')
await page.waitForTimeout(400)
html = await page.content()
console.log('card deleted:', !html.includes('What is 3+3?'))

// add three cards back for a Study Session
for (const [front, back] of [
  ['Q1', 'A1'],
  ['Q2', 'A2'],
  ['Q3', 'A3']
]) {
  await page.fill('input[placeholder="Front"]', front)
  await page.fill('input[placeholder="Back"]', back)
  await page.click('text=+ Add Card')
  await page.waitForTimeout(300)
}

// Browse mode: flip, Prev/Next
await page.click('text=Study')
await page.waitForTimeout(300)
await page.click('text=Browse')
await page.waitForTimeout(300)
html = await page.content()
console.log('browse shows card 1 front:', html.includes('Q1') && html.includes('Card 1 of 3'))
await page.click('.study-card')
await page.waitForTimeout(200)
html = await page.content()
console.log('browse flip shows back:', html.includes('A1'))
await page.click('text=Next')
await page.waitForTimeout(200)
html = await page.content()
console.log('browse next advances:', (await page.content()).includes('Card 2 of 3') && html.includes('Q2'))

// Practice mode: mark wrong/right, verify retry round and completion
await page.click('text=← Back')
await page.waitForTimeout(200)
await page.click('text=Study')
await page.waitForTimeout(200)
await page.click('text=Practice')
await page.waitForTimeout(300)

await page.click('.study-card') // flip Q1
await page.click('text=✗') // mark wrong
await page.waitForTimeout(200)
await page.click('.study-card') // flip Q2
await page.click('text=✓') // mark correct
await page.waitForTimeout(200)
await page.click('.study-card') // flip Q3
await page.click('text=✓') // mark correct, ends round 1 with one wrong card
await page.waitForTimeout(200)
html = await page.content()
console.log(
  'retry round has only the missed card:',
  html.includes('Round 2') && html.includes('Card 1 of 1') && html.includes('Q1')
)

await page.click('.study-card') // flip Q1 again
await page.click('text=✓') // mark correct, retry round ends clean
await page.waitForTimeout(200)
html = await page.content()
console.log('session completes after a clean retry round:', html.includes('Session complete'))

// back to the tree, create a second set with its own cards
await page.click('text=← Back')
await page.waitForTimeout(200)
await page.click('text=← Back')
await page.waitForTimeout(200)

await page.hover('.node-row')
await page.click('text=+ Set')
await page.waitForTimeout(400)
await page.keyboard.press('Enter')
await page.waitForTimeout(400)

const setNames = await page.$$('.node-name-clickable')
await setNames[1].click()
await page.waitForTimeout(400)
await page.fill('input[placeholder="Front"]', 'Q4')
await page.fill('input[placeholder="Back"]', 'A4')
await page.click('text=+ Add Card')
await page.waitForTimeout(300)
await page.click('text=← Back')
await page.waitForTimeout(300)

// Ad-hoc Study: select both sets, start a combined Browse session, nothing persisted
await page.click('text=Study Sets')
await page.waitForTimeout(200)
const checkboxes = await page.$$('.node-row input[type="checkbox"]')
for (const cb of checkboxes) await cb.click()
await page.waitForTimeout(200)
html = await page.content()
console.log('adhoc panel shows both sets selected:', html.includes('2 set(s) selected'))

await page.click('text=Start Study Session')
await page.waitForTimeout(300)
await page.click('text=Browse')
await page.waitForTimeout(300)
html = await page.content()
console.log('adhoc session combines cards from both sets:', html.includes('Card 1 of 4'))

await page.click('text=← Back')
await page.waitForTimeout(300)
html = await page.content()
console.log(
  'adhoc selection not retained after exit (Study Sets available again, no leftover selection):',
  html.includes('Study Sets') && !html.includes('set(s) selected')
)

await app.close()
