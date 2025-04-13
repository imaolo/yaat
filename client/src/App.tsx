import { useEffect, useState } from "react"
import axios from "axios"
import DocTabs from "@/components/DocTabs"

export interface Metadata {
  read: {
    title: string
    [key: string]: any
  }
  create?: any
  update?: any
  delete?: boolean
}

export default function App() {
  const [metadatas, setMetadatas] = useState<Metadata[]>([])

  useEffect(() => {
    axios
      .get("/metadatas")
      .then((res) => res.data)
      .then((mds) => mds.filter((md: Metadata) => md.read != null))
      .then(setMetadatas)
  }, [])

  return (
    <div className="w-screen h-screen overflow-hidden">
      <h1 className="text-2xl font-bold p-4">yaat club</h1>
      <DocTabs metadatas={metadatas} />
    </div>
  )
}
