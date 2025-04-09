import { useState } from 'react';
import { TabView, TabPanel } from 'primereact/tabview';
import DocTab from './DocTab'

export default function DocTabs({ metadatas }) {
    const [activeIndex, setActiveIndex] = useState(0)
    return (
        <div style={{ height: "100%", width: "100%", margin: "0 auto", overflowX: "auto" }}>
            <TabView activeIndex={activeIndex} onTabChange={(e) => setActiveIndex(e.index)}>
                {metadatas.map((metadata, idx) => (
                    <TabPanel key={idx} header={metadata.read.title}>
                        {activeIndex === idx && <DocTab metadata={metadata} />}
                    </TabPanel>
                ))}       
            </TabView>
        </div>
    )
}
