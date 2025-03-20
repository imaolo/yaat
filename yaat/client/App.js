import React from "react";
import axios from 'axios'
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import { Tabs, Tab, Box, Button, CircularProgress } from "@mui/material"
import { DataGrid } from "@mui/x-data-grid";
import $RefParser from "json-schema-ref-parser";
import { flatten } from 'flat';

function App() {
    return (
        <div>
            <h1>yaat club</h1>
            <DocTabs />
        </div>
    );
}

// helper
function getColumnsFromSchema(schema, prefix = "", result = []) {
    if (!schema || !schema.properties) return result;
  
    for (const [propName, propSchema] of Object.entries(schema.properties)) {
        const fullPath = prefix ? `${prefix}.${propName}` : propName;
        if (propSchema.type === "object" && propSchema.properties)
            getColumnsFromSchema(propSchema, fullPath, result);
        else
            result.push({
                field: fullPath,
                headerName: fullPath,
                flex: 1
            })
    }
  
    return result;
}

// all the tabs, gets the array of schemas
function DocTabs() {
    const [activeTab, setActiveTab] = React.useState(0);
    const [metadatas, setMetadatas] = React.useState([]);

    const handleChange = (event, newValue) => {
        setActiveTab(newValue);
    }

    React.useEffect(() => {
        axios.get('/metadatas')
            .then(res => res.data)
            .then(mds => mds.filter(md => md.read != null))
            .then(mds => setMetadatas(mds))
    }, [])

    return (
        <Box sx={{ width: "100%" }}>

            <Tabs value={activeTab} onChange={handleChange} aria-label="basic tabs">
                {metadatas.map((metadata, index) => (<Tab key={index} label={metadata.read.title} />))}
            </Tabs>

            <Box sx={{ p: 3 }}>
                {metadatas.map((metadata, index) => (
                    <div key={index} hidden={activeTab !== index}>
                        {activeTab === index && <DataTable metadata={metadata} />}
                    </div>
                ))}
            </Box>

        </Box>
    );
}

// datatable and forms, and delete, ... and edit
function DataTable({ metadata }) {
    const [rows, setRows] = React.useState([]);
    const [cols, setCols] = React.useState([]);
    const [loading, setLoading] = React.useState(true);
    const controllerRef = React.useRef(null);

    const cleanRows = (rows) => rows.map((item, index) => ({ id: index, ...item })).map(item => flatten(item))
    const fetchSetRows = () => {
        if (controllerRef.current)
            controllerRef.current.abort();

        const controller = new AbortController();
        controllerRef.current = controller

        const signal = controller.signal;
    
        setLoading(true)
        axios.get(`/${metadata.read.title}`, { signal })
            .then(res => setRows(cleanRows(res.data)))
            .catch(err => {
                if (axios.isCancel(err))
                    console.log("Request canceled:", err.message);
                else
                    console.error(err);
            })
            .finally(() => {
                if (controllerRef.current === controller)
                    setLoading(false);
            });
    }
    const handleSubmit = ({ formData }) => axios.post(`/${metadata.read.title}`, formData).then(res => fetchSetRows())
    const handleDelete = (params) => {
        axios.delete(`/${metadata.read.title}`, {params: {id: params.row._id}})
            .then(res => fetchSetRows())
            .catch(err => console.log(err))
    }
    const handleEdit = (params) => {
        console.log('TODO - edit')
        console.log(params)
    }
    const derefSetCols = () => {
        $RefParser.dereference(metadata.read).then(schema => {
            let new_cols = getColumnsFromSchema(schema)
            if (metadata.update != null | metadata.delete != null){
                new_cols.push({
                    field: "actions",
                    headerName: "Actions",
                    renderCell: (params) => (
                        <div>
                        {metadata.update != null && <Button variant="contained" color="primary" onClick={() => handleEdit(params)} style={{ marginRight: "8px" }}>
                            Edit
                        </Button>}
                        {metadata.delete != null && <Button variant="contained" color="secondary" onClick={() => handleDelete(params)}>
                            Delete
                        </Button>}
                        </div>
                    ),
                    sortable: false,
                    flex: 1,
                })
            }
            setCols(new_cols)
            return new_cols
        })
    }

    React.useEffect(() => {
        fetchSetRows();
        derefSetCols();

        return () => {
            if (controllerRef.current)
                controllerRef.current.abort();
        };
    }, []);

    return (
        <div>
            <h2>{metadata.read.title}</h2>
            {loading && (
                <div style={{ display: "flex", justifyContent: "center", alignItems: "center", height: "100px" }}>
                    <CircularProgress />
                </div>
            )}
            <div style={{ height: 400, width: "100%" }}>
                <DataGrid rows={rows} columns={cols} pageSize={5} />
            </div>
            <div>
                {metadata.create != null && <Form schema={metadata.create} validator={validator} onSubmit={handleSubmit} />}
            </div>
        </div>
    );
}

export default App;


// edit row stuff

//   // Edit row handler (could open a modal or enable inline editing)
//   const handleEdit = (row) => {
//     setEditRow(row);
//   };

//   // Save modifications for a row (if using a separate form for editing)
//   const handleUpdate = (updatedData) => {
//     axios
//       .put(`/${metadata.schema.title}/${editRow.id}`, updatedData)
//       .then(() => {
//         setEditRow(null);
//         fetchSetRows();
//       });
//   };


//   // Add an actions column with edit and delete buttons

//   // Combine columns
//   const allColumns = [...columns, actionColumn];

//   return (
//     <div>
//       <h2>{metadata.schema.title}</h2>
//       <div style={{ height: 400, width: "100%" }}>
//         <DataGrid rows={rows} columns={allColumns} pageSize={5} />
//       </div>
//       <div>
//         {/* Create new entry */}
//         {metadata.createable && (
//           <Form
//             schema={metadata.schema}
//             validator={validator}
//             onSubmit={handleSubmit}
//           />
//         )}
//       </div>
//       <div>
//         {/* Edit form (could be a modal instead) */}
//         {editRow && (
//           <div>
//             <h3>Edit {metadata.schema.title}</h3>
//             <Form
//               schema={metadata.schema}
//               formData={editRow}
//               validator={validator}
//               onSubmit={({ formData }) => handleUpdate(formData)}
//             />
//             <Button onClick={() => setEditRow(null)}>Cancel</Button>
//           </div>
//         )}
//       </div>
//     </div>
//   );
// }

// export default App;

