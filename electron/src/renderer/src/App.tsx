import { useState } from 'react'
import GroupTree from './GroupTree'
import CardList from './CardList'
import type { Set } from '../../preload'

function App(): React.JSX.Element {
  const [openSet, setOpenSet] = useState<Set | null>(null)

  if (openSet) {
    return <CardList set={openSet} onBack={() => setOpenSet(null)} />
  }
  return <GroupTree onOpenSet={setOpenSet} />
}

export default App
