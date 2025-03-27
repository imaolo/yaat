import { useState, useEffect, useRef } from 'react';
import { TabView, TabPanel } from 'primereact/tabview';
import { DataTable } from 'primereact/datatable';
import { Column }    from 'primereact/column';
import $RefParser from "json-schema-ref-parser";
import axios from 'axios'
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";

const filterOptions = {
  string: [
    { label: 'Contains', value: 'contains' },
    { label: 'Equals', value: 'equals' },
    { label: 'Not Equals', value: 'notEquals' }
  ],
  number: [
    { label: '=', value: 'equals' },
    { label: '≠', value: 'notEquals' },
    { label: '<', value: 'lt' },
    { label: '<=', value: 'lte' },
    { label: '>', value: 'gt' },
    { label: '>=', value: 'gte' }
  ],
  boolean: [
    { label: 'True', value: true },
    { label: 'False', value: false }
  ]
};

function DocTab({ metadata }){
    const [rows, setRows] = useState([]);
    const [first, setFirst] = useState(0);
    const [size, setSize] = useState(25);
    const [total, setTotal] = useState(0);
    const [readSchema, setReadSchema] = useState({});

    // manage call stage
    const controllerRef = useRef(null);
    const abortCall = () => {
        if (controllerRef.current)
            controllerRef.current.abort()
    }

    // form submission
    const handleSubmit = ({ formData }) => 
        axios
            .post(`/${metadata.read.title}`, formData)
            .then(() => setFirst(first))

    // mount effect
    useEffect(() => {
        $RefParser
            .dereference(metadata.read)
            .then(schema => setReadSchema(schema))
        return abortCall
    }, [])



    useEffect(() => {
        const page = Math.floor(first / size) + 1;
        const params = new URLSearchParams({ page, size })
        const controller = new AbortController()
        const signal = controller.signal;

        abortCall()
        controllerRef.current = controller

        axios
            .get(`/${metadata.read.title}`, { signal, params })
            .then(res => {
                console.log(res.data.items)
                setRows(res.data.items)
                setTotal(res.data.total)
            })
            .catch(err => {
                if (!axios.isCancel(err)){
                    console.log(err)
                    throw err
                }
            })
    }, [first, size, readSchema]);

    
    const getColsFromSchema = (schema, prefix = "", result = [])  => {
        console.log(schema)
        if (!schema || !schema.properties) return result;
    
        for (const [propName, propSchema] of Object.entries(schema.properties)) {
            const fullPath = prefix ? `${prefix}.${propName}` : propName;
            if (propSchema.type === "object" && propSchema.properties)
                getColsFromSchema(propSchema, fullPath, result);
            else
                result.push(<Column
                    field={fullPath}
                    header={fullPath}
                    sortable
                />)
        }
        return result
    }

    return (
        <div>
          <DataTable
            value={rows}
            // visual
            scrollable
            scrollHeight="70vh"
            resizableColumns
            columnResizeMode='expand'
            reorderableColumns
            // lazy loading
            lazy
            // paging
            paginator
            first={first}
            totalRecords={total}
            rows={size}
            onPage={(e) => {
                setFirst(e.first)
                setSize(e.rows)
            }}
            rowsPerPageOptions={[25, 50, 100]}
            paginatorTemplate="RowsPerPageDropdown FirstPageLink PrevPageLink CurrentPageReport NextPageLink LastPageLink"
            currentPageReportTemplate="{first} to {last} of {totalRecords}"
            // TODO - sorting
          >
            {getColsFromSchema(readSchema)}
          </DataTable>
          <div>
                {metadata.create != null && 
                    <Form
                        schema={metadata.create}
                        validator={validator}
                        onSubmit={handleSubmit}
                    />
                }
            </div>
        </div>
      );
}

{/* <DataTable
value={rows}
paginator
lazy
removableSort
first={lazyParams.first}
rows={lazyParams.rows}
totalRecords={totalRows}
rowsPerPageOptions={[25, 50, 100]}
filterDisplay="menu"
scrollable            // <-- enable scrolling
scrollHeight="60vh"    
onPage={(e) => setLazyParams({ first: e.first, rows: e.rows })}
> */}

export default function DocTabs({ metadatas }) {
    const [activeIndex, setActiveIndex] = useState(0)

    return (
        <TabView activeIndex={activeIndex} onTabChange={(e) => setActiveIndex(e.index)}>
            {metadatas.map((metadata, idx) => (
                <TabPanel key={idx} header={metadata.read.title}>
                    {activeIndex === idx && <DocTab metadata={metadata} />}
                </TabPanel>
            ))}       
        </TabView>
    )
}
