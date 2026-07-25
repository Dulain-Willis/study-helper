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

await app.close()
