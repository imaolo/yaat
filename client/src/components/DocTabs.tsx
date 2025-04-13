// src/components/DocTabs.tsx
// import 'ag-grid-community/styles/ag-theme-quartz-dark.css'; // ✅

// import 'ag-grid-community/styles/ag-grid.css';
// import 'ag-grid-community/styles/ag-theme-quartz-dark.css';

// import 'ag-grid-community/styles/ag-grid.css';
// import '#modules/ag-grid-community/styles/ag-theme-quartz.css';






// import 'ag-grid-community/styles/ag-grid.css'
// import 'ag-grid-community/styles/ag-theme-alpine.css'
// import 'ag-grid-community/styles/ag-theme-alpine-dark.css'

// import 'ag-grid-community/styles/ag-grid.css';
// import 'ag-grid-community/styles/ag-theme-alpine.css';
// import 'ag-grid-community/styles/themes/ag-theme-alpine-dark.css'; // ✅ FIXED path

// import 'ag-grid-community/styles/ag-grid.css';
// import 'ag-grid-community/styles/ag-theme-alpine-dark.css';

// import 'ag-grid-community/styles/ag-theme-quartz-dark.css';

import { useState } from "react"
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs"
import DocTab from "@/components/DocTab"
import type { Metadata } from "@/App"


export default function DocTabs({ metadatas }: { metadatas: Metadata[] }) {
  const [activeTab, setActiveTab] = useState<string | null>(
    metadatas[0]?.read.title ?? null
  )

  return (
    <div className="ag-theme-quartz">
        <Tabs
        value={activeTab ?? undefined}
        onValueChange={(value) => setActiveTab(value)}
        className="w-full h-full px-4"
        >
        <TabsList className="overflow-x-auto whitespace-nowrap max-w-full">
            {metadatas.map((metadata) => (
            <TabsTrigger key={metadata.read.title} value={metadata.read.title}>
                {metadata.read.title}
            </TabsTrigger>
            ))}
        </TabsList>

        {metadatas.map((metadata) => (
            <TabsContent key={metadata.read.title} value={metadata.read.title}>
                <DocTab metadata={metadata} />
            </TabsContent>
        ))}
        </Tabs>
    </div>
  )
}
