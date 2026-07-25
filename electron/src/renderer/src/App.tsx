import { useState } from 'react'
import GroupTree from './GroupTree'
import CardList from './CardList'
import StudySession from './StudySession'
import type { Set } from '../../preload'

function App(): React.JSX.Element {
  const [openSet, setOpenSet] = useState<Set | null>(null)
  const [studying, setStudying] = useState(false)

  if (openSet && studying) {
    return <StudySession set={openSet} onExit={() => setStudying(false)} />
  }
  if (openSet) {
    return (
      <CardList set={openSet} onBack={() => setOpenSet(null)} onStudy={() => setStudying(true)} />
    )
  }
  return <GroupTree onOpenSet={setOpenSet} />
}

export default App
