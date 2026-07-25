import { useState } from 'react'
import GroupTree from './GroupTree'
import CardList from './CardList'
import StudySession from './StudySession'
import type { Set } from '../../preload'

type AdhocStudy = { title: string; setIds: number[] }

function App(): React.JSX.Element {
  const [openSet, setOpenSet] = useState<Set | null>(null)
  const [studying, setStudying] = useState(false)
  const [adhocStudy, setAdhocStudy] = useState<AdhocStudy | null>(null)

  if (adhocStudy) {
    return (
      <StudySession
        title={adhocStudy.title}
        setIds={adhocStudy.setIds}
        onExit={() => setAdhocStudy(null)}
      />
    )
  }
  if (openSet && studying) {
    return (
      <StudySession title={openSet.name} setIds={[openSet.id]} onExit={() => setStudying(false)} />
    )
  }
  if (openSet) {
    return (
      <CardList set={openSet} onBack={() => setOpenSet(null)} onStudy={() => setStudying(true)} />
    )
  }
  return <GroupTree onOpenSet={setOpenSet} onStudyAdhoc={setAdhocStudy} />
}

export default App
