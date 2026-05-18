import { YDocProvider } from '@arcterus-jp/react'
import { randomId } from '@/lib/utils'
import { CodeEditor } from './CodeEditor'

export default function Home({ searchParams }: { searchParams: { doc: string } }) {
  const docId = searchParams.doc ?? randomId()
  return (
    <YDocProvider docId={docId} setQueryParam="doc" authEndpoint="/api/auth" offlineSupport={true}>
      <CodeEditor />
    </YDocProvider>
  )
}
