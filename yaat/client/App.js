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
// import Form from "@rjsf/core";
// import validator from "@rjsf/validator-ajv8";
// import { Tabs, Tab, Box, Button, CircularProgress } from "@mui/material"
// // import { DataGrid,  getGridNumericOperators } from "@mui/x-data-grid";
// import $RefParser from "json-schema-ref-parser";
// import { flatten } from 'flat';
// // import { AgGridReact } from "ag-grid-react";

// import { AgGridReact } from 'ag-grid-react';
// import { InfiniteRowModelModule, ClientSideRowModelModule, AllCommunityModule, ModuleRegistry} from 'ag-grid-community';
// import 'ag-grid-community/styles/ag-grid.css'; // Core grid CSS
// import 'ag-grid-community/styles/ag-theme-alpine.css'; // Theme CSS


// import { ReactTabulator } from 'react-tabulator';
// import 'react-tabulator/lib/styles.css';
// import 'react-tabulator/lib/css/tabulator.min.css';

// import { DataTable } from 'primereact/datatable';
// import { Column } from 'primereact/column';

// // PrimeReact styles (make sure these are imported once in your app)
// import 'primereact/resources/themes/saga-blue/theme.css';
// import 'primereact/resources/primereact.min.css';
// import 'primeicons/primeicons.css';

// const rowData = [
//     { make: 'make-12x', model: 'model-12y', price: 35000 },
//     { make: 'make-13x', model: 'model-13x', price: 32000 },
//     { make: 'make-14y', model: 'model-14x', price: 72000 },
//     { make: 'make-12x', model: 'model-12y', price: 35000 },
//     { make: 'make-13x', model: 'model-13x', price: 32000 },

// ];

// function getColsFromSchema(schema, prefix = "", result = []) {
//     if (!schema || !schema.properties) return result;
  
//     for (const [propName, propSchema] of Object.entries(schema.properties)) {
//         const fullPath = prefix ? `${prefix}.${propName}` : propName;
//         if (propSchema.type === "object" && propSchema.properties)
//             getColsFromSchema(propSchema, fullPath, result);
//         else
//             result.push(<Column field={fullPath} header={fullPath} filter />)
//    }
//    return result
// }

// const App = () => {

//     return (
//         <div className="p-m-4">
//           <DataTable 
//             value={rowData} 
//             paginator 
//             rows={25}
//             rowsPerPageOptions={[25, 50, 100, 1000]} // dropdown options
//             filterDisplay="menu"
//           >
//           {getColsFromSchema()}
//           </DataTable>
//         </div>
//       );
// };



// // datagrid columm definition helper
// function getColsFromSchema(schema, prefix = "", result = []) {
//     if (!schema || !schema.properties) return result;
  
//     for (const [propName, propSchema] of Object.entries(schema.properties)) {
//         const fullPath = prefix ? `${prefix}.${propName}` : propName;
//         if (propSchema.type === "object" && propSchema.properties)
//             getColsFromSchema(propSchema, fullPath, result);
//         else
//             result.push(<Column field={fullPath} header={fullPath} filter />)
//    }
//    return result
// }

// // all the tabs, gets the array of schemas
// function DocTabs() {
//     const [activeTab, setActiveTab] = useState(0);
//     const [metadatas, setMetadatas] = useState([]);

//     const handleChange = (event, newValue) => {
//         setActiveTab(newValue);
//     }

//     useEffect(() => {
//         axios.get('/metadatas')
//             .then(res => res.data)
//             .then(mds => mds.filter(md => md.read != null))
//             .then(mds => setMetadatas(mds))
//     }, [])

//     return (
//         <Box sx={{ width: "100%" }}>

//             <Tabs value={activeTab} onChange={handleChange} aria-label="basic tabs">
//                 {metadatas.map((metadata, index) => (<Tab key={index} label={metadata.read.title} />))}
//             </Tabs>

//             <Box sx={{ p: 3 }}>
//                 {metadatas.map((metadata, index) => (
//                     <div key={index} hidden={activeTab !== index}>
//                         {activeTab === index && <DocTab metadata={metadata} />}
//                     </div>
//                 ))}
//             </Box>

//         </Box>
//     );
// }

// // datatable and forms, and delete, ... and edit
// function DocTab({ metadata }) {
//     const [totalRecords, setTotalRecords] = useState(0);
//     const [lazyParams, setLazyParams] = useState({ first: 0, rows: 25 });
//     const [rows, setRows] = React.useState([]);
//     // const [cols, setCols] = React.useState([]);
//     // const [loading, setLoading] = React.useState(true);
//     // const [pageSize] = React.useState(50);
//     // const gridApiRef = React.useRef(null);
//     const controllerRef = useRef(null);

//     const cleanRows = (rows) => 
//         rows
//             .map((item, index) => ({ id: index, ...item }))
//             .map(item => flatten(item))

//     // AG Grid calls this function when the grid is ready.
//     // ── MIGRATION NOTE ──
//     // Instead of DataGrid props for pagination and filtering, we set up a server-side data source.

//     // const fetchSetRows = (currentPage = 1, currentSize = 50) => {
//     //     if (controllerRef.current)
//     //         controllerRef.current.abort();

//     //     const controller = new AbortController();
//     //     controllerRef.current = controller

//     //     const signal = controller.signal;
    
//     //     setLoading(true)
//     //     axios.get(`/${metadata.read.title}`, { signal, params: {page: currentPage, size: currentSize}})
//     //         .then(res => {
//     //             setRowCount(res.data.total)
//     //             setRows(cleanRows(res.data.items))
//     //         })
//     //         .catch(err => {
//     //             if (axios.isCancel(err))
//     //                 console.log("Request canceled:", err.message);
//     //             else
//     //                 throw err;
//     //         })
//     //         .finally(() => {
//     //             if (controllerRef.current === controller)
//     //                 setLoading(false);
//     //         });
//     // }

//     const handleSubmit = ({ formData }) => 
//         axios
//             .post(`/${metadata.read.title}`, formData)
//             .then((res) => gridApiRef.current.refreshServerSideStore())

//     // const handleDelete = (params) => {
//     //     axios
//     //         .delete(`/${metadata.read.title}`, {params: {id: params.row._id}})
//     //         .then((res) => gridApiRef.current.refreshServerSideStore())
//     // }
//     // const handleEdit = (params) => {
//     //     console.log('TODO - edit')
//     //     console.log(params)
//     // }
//     // const derefSetCols = () => {
//     //     $RefParser
//     //         .dereference(metadata.read)
//     //         .then(schema => {
//     //             let new_cols = getColsFromSchema(schema)
//     //             if (metadata.update != null | metadata.delete != null){
//     //                 new_cols.push({
//     //                     field: "actions",
//     //                     headerName: "Actions",
//     //                     renderCell: (params) => (
//     //                         <div>
//     //                         {metadata.update != null && <Button variant="contained" color="primary" onClick={() => handleEdit(params)} style={{ marginRight: "8px" }}>
//     //                             Edit
//     //                         </Button>}
//     //                         {metadata.delete != null && <Button variant="contained" color="secondary" onClick={() => handleDelete(params)}>
//     //                             Delete
//     //                         </Button>}
//     //                         </div>
//     //                     ),
//     //                     sortable: false,
//     //                     flex: 1,
//     //                 })
//     //             }
//     //             setCols(new_cols)
//     //         })
//     // }

//     useEffect(() => {
//         const page = lazyParams.first / lazyParams.rows;
//         // const params = new URLSearchParams({ page, pageSize: lazyParams.rows });

//         if (controllerRef.current)
//             controllerRef.current.abort();

//         const controller = new AbortController();
//         controllerRef.current = controller

//         const signal = controller.signal;

//         // console.log(params)
//         axios
//             .get(`/${metadata.read.title}`, { signal, params: { page, pageSize: lazyParams.rows } })
//             .then(res => {
//                 setRows(cleanRows(res.data.items))
//                 setTotalRecords(res.data.total)
//             })
//             .catch(err => console.log(err))

//         return () => {
//             if (controllerRef.current)
//                 controllerRef.current.abort();
//         };
//     }, [lazyParams]);

//       // Called when user navigates pages
//     const onPage = (event) => {
//         setLazyParams({ first: event.first, rows: event.rows });
//     };

//     //     // derefSetCols();

//     //     return () => {
//     //         if (controllerRef.current)
//     //             controllerRef.current.abort();
//     //     };
//     // }, [metadata]);


//     // TODO 
//     return (
//         <div>
//             <h2>{metadata.read.title}</h2>
//             <div style={{ height: 400, width: "100%" }}>
//                 <DataTable
//                     value={rows}
//                     paginator
//                     lazy
//                     first={lazyParams.first}
//                     rows={lazyParams.rows}
//                     totalRecords={totalRecords}
//                     rowsPerPageOptions={[25, 50, 100]}
//                     filterDisplay="menu"
//                     onPage={onPage}
//                 >
//                     {getColsFromSchema()}
//                 </DataTable>
//             </div>
//             <div>
//                 {metadata.create != null && 
//                     <Form
//                         schema={metadata.create}
//                         validator={validator}
//                         onSubmit={handleSubmit}
//                     />
//                 }
//             </div>
//         </div>
//     );
// }



// //     const [filterModel, setFilterModel] = React.useState({ items: [] });

// // filterMode="server"
// // onFilterModelChange = {(newFilterModel, details) => {
// //     setFilterModel(newFilterModel);
// //     const filters = newFilterModel.items.reduce((acc, filter) => {
// //         if (filter.value) acc[filter.columnField] = filter.value;
// //         return acc;
// //     }, {});
// //     fetchSetRows(1, pageSize, filters); // Reset to the first page when filters change
// // }}

// // edit row stuff

// //   // Edit row handler (could open a modal or enable inline editing)
// //   const handleEdit = (row) => {
// //     setEditRow(row);
// //   };

// //   // Save modifications for a row (if using a separate form for editing)
// //   const handleUpdate = (updatedData) => {
// //     axios
// //       .put(`/${metadata.schema.title}/${editRow.id}`, updatedData)
// //       .then(() => {
// //         setEditRow(null);
// //         fetchSetRows();
// //       });
// //   };


// //   // Add an actions column with edit and delete buttons

// //   // Combine columns
// //   const allColumns = [...columns, actionColumn];

// //   return (
// //     <div>
// //       <h2>{metadata.schema.title}</h2>
// //       <div style={{ height: 400, width: "100%" }}>
// //         <DataGrid rows={rows} columns={allColumns} pageSize={5} />
// //       </div>
// //       <div>
// //         {/* Create new entry */}
// //         {metadata.createable && (
// //           <Form
// //             schema={metadata.schema}
// //             validator={validator}
// //             onSubmit={handleSubmit}
// //           />
// //         )}
// //       </div>
// //       <div>
// //         {/* Edit form (could be a modal instead) */}
// //         {editRow && (
// //           <div>
// //             <h3>Edit {metadata.schema.title}</h3>
// //             <Form
// //               schema={metadata.schema}
// //               formData={editRow}
// //               validator={validator}
// //               onSubmit={({ formData }) => handleUpdate(formData)}
// //             />
// //             <Button onClick={() => setEditRow(null)}>Cancel</Button>
// //           </div>
// //         )}
// //       </div>
// //     </div>
// //   );
// // }

// // export default App;

