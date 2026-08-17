import { _electron as electron } from 'playwright-core'

let app = await electron.launch({
  args: ['--user-data-dir=/tmp/study-helper-smoke', '.'],
  cwd: process.cwd()
})
let page = await app.firstWindow()
page.on('console', (m) => console.log('[renderer]', m.text()))
page.on('pageerror', (e) => console.log('[pageerror]', e))
await page.waitForTimeout(1500)

// Root page: create a group, navigate into it
await page.fill('input[placeholder="New group name"]', 'New Group')
await page.click('text=+ Group')
await page.waitForTimeout(400)
await page.click('.node-name-clickable')
await page.waitForTimeout(400)
let html = await page.content()
console.log('breadcrumb shows group name after navigating in:', html.includes('New Group'))

// Inside group: create a set, open it
await page.fill('input[placeholder="New set name"]', 'New Set')
await page.click('text=+ Set')
await page.waitForTimeout(400)
await page.screenshot({ path: '/tmp/shots-01-set-created.png' })

await page.click('.node-name-clickable:has-text("New Set")')
await page.waitForTimeout(500)
await page.screenshot({ path: '/tmp/shots-02-cardlist-empty.png' })

// add a card
await page.fill('textarea[placeholder="Front"]', 'What is 2+2?')
await page.fill('textarea[placeholder="Back"]', '4')
await page.click('text=+ Add Card')
await page.waitForTimeout(400)
await page.screenshot({ path: '/tmp/shots-03-card-added.png' })

html = await page.content()
console.log('card shown after add:', html.includes('What is 2+2?') && html.includes('>4<'))

// edit the card
await page.click('text=Edit')
await page.waitForTimeout(300)
const inputs = await page.$$('.card-row textarea')
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
  await page.fill('textarea[placeholder="Front"]', front)
  await page.fill('textarea[placeholder="Back"]', back)
  await page.click('text=+ Add Card')
  await page.waitForTimeout(300)
}

// Browse mode: flip, Prev/Next
await page.click('.card-list-header >> text=Study')
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
console.log(
  'browse next advances:',
  (await page.content()).includes('Card 2 of 3') && html.includes('Q2')
)

// Practice mode: mark wrong/right, verify retry round and completion
await page.click('text=← Back')
await page.waitForTimeout(200)
await page.click('.card-list-header >> text=Study')
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

// back to the group page, create a second set with its own cards
await page.click('text=Back to Set')
await page.waitForTimeout(200)
await page.click('text=← Back')
await page.waitForTimeout(200)

await page.fill('input[placeholder="New set name"]', 'Second Set')
await page.click('text=+ Set')
await page.waitForTimeout(400)

await page.click('.node-name-clickable:has-text("Second Set")')
await page.waitForTimeout(400)
await page.fill('textarea[placeholder="Front"]', 'Q4')
await page.fill('textarea[placeholder="Back"]', 'A4')
await page.click('text=+ Add Card')
await page.waitForTimeout(300)
await page.click('text=← Back')
await page.waitForTimeout(300)

// Ad-hoc Study: select both sets, start a combined Browse session, nothing persisted
await page.click('text=Study Sets')
await page.waitForTimeout(200)
const checkboxes = await page.$$('input[type="checkbox"]')
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

// Practice session persistence (#36): mark a card wrong on Second Set, force-quit,
// relaunch pointed at the same user-data-dir, and confirm the session resumes.
await page.click('.node-name-clickable:has-text("Second Set")')
await page.waitForTimeout(400)
await page.click('.card-list-header >> text=Study')
await page.waitForTimeout(200)
await page.click('text=Practice')
await page.waitForTimeout(300)
await page.click('.study-card') // flip Q4
await page.click('text=✗') // mark wrong
await page.waitForTimeout(300)
await app.close()

const app2 = await electron.launch({
  args: ['--user-data-dir=/tmp/study-helper-smoke', '.'],
  cwd: process.cwd()
})
const page2 = await app2.firstWindow()
await page2.waitForTimeout(1500)
await page2.click('.node-name-clickable:has-text("New Group")')
await page2.waitForTimeout(400)
await page2.click('.node-name-clickable:has-text("Second Set")')
await page2.waitForTimeout(400)
await page2.click('.card-list-header >> text=Study')
await page2.waitForTimeout(400)
html = await page2.content()
console.log(
  'practice session resumes after force-quit:',
  html.includes('Round 2') && html.includes('Card 1 of 1') && html.includes('0 / 1')
)
await app2.close()

const app3 = await electron.launch({
  args: ['--user-data-dir=/tmp/study-helper-smoke', '.'],
  cwd: process.cwd()
})
const page3 = await app3.firstWindow()
await page3.waitForTimeout(1500)
await page3.click('.node-name-clickable:has-text("New Group")')
await page3.waitForTimeout(400)
await page3.click('.node-name-clickable:has-text("Second Set")')
await page3.waitForTimeout(400)
await page3.click('.card-list-header >> text=Study')
await page3.waitForTimeout(400)
await page3.click('.study-card') // flip Q4 (resumed round 2)
await page3.click('text=✓') // mark correct, session completes, progress cleared
await page3.waitForTimeout(300)
await page3.click('text=Back to Set')
await page3.waitForTimeout(300)
await page3.click('.card-list-header >> text=Study')
await page3.waitForTimeout(400)
html = await page3.content()
console.log(
  'completed session clears persisted progress (fresh pick screen shown):',
  html.includes('Practice')
)
await app3.close()

app = await electron.launch({
  args: ['--user-data-dir=/tmp/study-helper-smoke', '.'],
  cwd: process.cwd()
})
page = await app.firstWindow()
await page.waitForTimeout(1500)
await page.click('.node-name-clickable:has-text("New Group")')
await page.waitForTimeout(400)

// Merge Sets: combine both sets into a new one, verify card count
await page.click('text=Merge Sets')
await page.waitForTimeout(200)
const mergeCheckboxes = await page.$$('input[type="checkbox"]')
for (const cb of mergeCheckboxes) await cb.click()
await page.fill('input[placeholder="New set name"]', 'Merged Set')
await page.selectOption('select', { index: 1 })
await page.click('.merge-panel >> text=Merge')
await page.waitForTimeout(400)

// merge mode exits back to whichever group page was active (New Group, unchanged throughout)
html = await page.content()
console.log('merged set appears in tree:', html.includes('Merged Set'))

await page.click('.node-name-clickable:has-text("Merged Set")')
await page.waitForTimeout(400)
html = await page.content()
console.log(
  'merged set contains all 4 source cards:',
  ['Q1', 'Q2', 'Q3', 'Q4'].every((q) => html.includes(q))
)

await app.close()
