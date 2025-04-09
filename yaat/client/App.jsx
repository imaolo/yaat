import React from "react";
import axios from 'axios'
import DocTabs from './DocTabs'

export default function App() {
    const [metadatas, setMetadatas] = React.useState([]);

    React.useEffect(() => {
        axios.get('/metadatas')
            .then(res => res.data)
            .then(mds => mds.filter(md => md.read != null))
            .then(mds => setMetadatas(mds))
    }, [])

    return (
        <div style={{ height: "100%", width: "100vw", overflow: 'hidden'}}>
            <h1 style={{ margin: 0, padding: "1rem" }}>yaat club</h1>
            <div >
                <DocTabs metadatas={metadatas} />
            </div>
        </div>
    );
}