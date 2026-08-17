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

  // GroupTree tracks its own navigation (current group, breadcrumb) as local
  // state, so it stays mounted at all times and is only hidden — swapping it
  // out via conditional rendering would reset that navigation to root every
  // time a set or study session is opened and closed.
  const showTree = !openSet && !studying && !adhocStudy

  return (
    <>
      <header className="app-header">
        <h1>Study Helper</h1>
      </header>
      <main className="app-main">
        <div style={{ display: showTree ? 'contents' : 'none' }}>
          <GroupTree onOpenSet={setOpenSet} onStudyAdhoc={setAdhocStudy} />
        </div>
        {adhocStudy && (
          <StudySession
            title={adhocStudy.title}
            setIds={adhocStudy.setIds}
            onExit={() => setAdhocStudy(null)}
          />
        )}
        {openSet && studying && (
          <StudySession
            title={openSet.name}
            setIds={[openSet.id]}
            setId={openSet.id}
            onExit={() => setStudying(false)}
          />
        )}
        {openSet && !studying && (
          <CardList set={openSet} onBack={() => setOpenSet(null)} onStudy={() => setStudying(true)} />
        )}
      </main>
    </>
  )
}

export default App
