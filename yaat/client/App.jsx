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
        <div style={{ width: '90vw' }}>
            <h1>yaat club</h1>
            <DocTabs metadatas={metadatas}/>
        </div>
    );
}