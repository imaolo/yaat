import React from "react";
import axios from 'axios'
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import { Tabs, Tab, Box } from "@mui/material"
import { DataGrid } from "@mui/x-data-grid";

function App() {
    return (
      <div>
        <h1>yaat club</h1>
        <DocTabs />
      </div>
    );
}

function DocTabs() {
    const [activeTab, setActiveTab] = React.useState(0);
    const [metadatas, setMetadatas] = React.useState([]);

    const handleChange = (event, newValue) => {
        setActiveTab(newValue);
    };

    React.useEffect(() => {
        axios.get('/metadatas').then(res => setMetadatas(res.data))
    }, [])

    return (
        <Box sx={{ width: "100%" }}>

            <Tabs value={activeTab} onChange={handleChange} aria-label="basic tabs">{
                metadatas.map((metadata, index) => (<Tab key={index} label={metadata.schema.title} />))
            }
            </Tabs>

            <Box sx={{ p: 3 }}>{
                metadatas.map((metadata, index) => (
                    <div key={index} hidden={activeTab !== index}>
                        {activeTab === index && <DataTable metadata={metadata} />}
                    </div>
                ))
            }
            </Box>

        </Box>
    );
}

function DataTable({ metadata }) {
    const [rows, setRows] = React.useState([]);

    const cleanRows = (rows) => rows.map(({ _id, ...rest }) => rest).map((item, index) => ({ id: index, ...item }));
    const fetchSetRows = () => axios.get(`/${metadata.schema.title}`).then(res => setRows(cleanRows(res.data)))
    const handleSubmit = ({ formData }) => axios.post(`/${metadata.schema.title}`, formData).then(res => fetchSetRows())

    React.useEffect(() => {
        fetchSetRows()
    }, [])

    const dataColumns = Object.keys(metadata.schema.properties).map((key) => ({
        field: key,
        headerName: key.charAt(0).toUpperCase() + key.slice(1),
        flex: 1,
    }));

    const actionColumn = {
        field: "actions",
        headerName: "Actions",
        renderCell: (params) => {
            const row = params.row;
            return (
                <div>
                    <p> yerrr </p>
                </div>
            )
        },
        sortable: false,
        flex: 1,
    };

    const columns = [...dataColumns, actionColumn];

    return (
        <div>
            <h2>{metadata.schema.title}</h2>
            <div style={{ height: 400, width: "100%" }}>
                <DataGrid rows={rows} columns={columns} pageSize={5} />
            </div>
            <div>
                {metadata.createable && <Form schema={metadata.schema} validator={validator} onSubmit={handleSubmit}/>}
            </div>
        </div>
    );
}

export default App;

// import React from "react";
// import axios from "axios";
// import Form from "@rjsf/core";
// import validator from "@rjsf/validator-ajv8";
// import { Tabs, Tab, Box, Button } from "@mui/material";
// import { DataGrid } from "@mui/x-data-grid";

// function App() {
//   return (
//     <div>
//       <h1>yaat club</h1>
//       <DocTabs />
//     </div>
//   );
// }

// function DocTabs() {
//   const [activeTab, setActiveTab] = React.useState(0);
//   const [metadatas, setMetadatas] = React.useState([]);

//   const handleChange = (event, newValue) => {
//     setActiveTab(newValue);
//   };

//   React.useEffect(() => {
//     axios.get("/metadatas").then((res) => setMetadatas(res.data));
//   }, []);

//   return (
//     <Box sx={{ width: "100%" }}>
//       <Tabs value={activeTab} onChange={handleChange} aria-label="basic tabs">
//         {metadatas.map((metadata, index) => (
//           <Tab key={index} label={metadata.schema.title} />
//         ))}
//       </Tabs>
//       <Box sx={{ p: 3 }}>
//         {metadatas.map((metadata, index) => (
//           <div key={index} hidden={activeTab !== index}>
//             {activeTab === index && <DataTable metadata={metadata} />}
//           </div>
//         ))}
//       </Box>
//     </Box>
//   );
// }

// function DataTable({ metadata }) {
//   const [rows, setRows] = React.useState([]);
//   const [editRow, setEditRow] = React.useState(null);

//   // Helper to clean rows and assign unique ids
//   const cleanRows = (rows) =>
//     rows
//       .map(({ _id, ...rest }) => rest)
//       .map((item, index) => ({ id: index, ...item }));

//   const fetchSetRows = () =>
//     axios.get(`/${metadata.schema.title}`).then((res) => setRows(cleanRows(res.data)));

//   // Create new entry
//   const handleSubmit = ({ formData }) =>
//     axios.post(`/${metadata.schema.title}`, formData).then(() => fetchSetRows());

//   React.useEffect(() => {
//     fetchSetRows();
//   }, []);

//   // Delete row handler
//   const handleDelete = (id) => {
//     // Assuming each row has a unique identifier (replace 'id' with your actual id field if needed)
//     axios.delete(`/${metadata.schema.title}/${id}`).then(() => {
//       fetchSetRows();
//     });
//   };

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

//   // Build columns based on metadata schema
//   const columns = Object.keys(metadata.schema.properties).map((key) => ({
//     field: key,
//     headerName: key.charAt(0).toUpperCase() + key.slice(1),
//     flex: 1,
//   }));

//   // Add an actions column with edit and delete buttons
//   const actionColumn = {
//     field: "actions",
//     headerName: "Actions",
//     flex: 1,
//     renderCell: (params) => (
//       <div>
//         <Button
//           variant="contained"
//           color="primary"
//           onClick={() => handleEdit(params.row)}
//           style={{ marginRight: "8px" }}
//         >
//           Edit
//         </Button>
//         <Button
//           variant="contained"
//           color="secondary"
//           onClick={() => handleDelete(params.row.id)}
//         >
//           Delete
//         </Button>
//       </div>
//     ),
//   };

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

