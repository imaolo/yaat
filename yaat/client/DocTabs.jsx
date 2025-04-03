import { useState, useEffect, useRef } from 'react';
import { TabView, TabPanel } from 'primereact/tabview';
import { DataTable } from 'primereact/datatable';
import { Column }    from 'primereact/column';
import { Button } from 'primereact/button';
import $RefParser from "json-schema-ref-parser";
import axios from 'axios'
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";

function DocTab({ metadata }){
    const [rows, setRows] = useState([]);
    const [selectedRows, setSelectedRows] = useState([]);
    const [first, setFirst] = useState(0);
    const [size, setSize] = useState(25);
    const [total, setTotal] = useState(0);
    const [readSchema, setReadSchema] = useState({});
    const [loading, setLoading] = useState(true)

    // manage api call state
    const controllerRef = useRef(null);
    const abortFetch = () => {
        if (controllerRef.current)
            controllerRef.current.abort()
    }

    // fetch Data helper
    const fetchData = () => {
        const page = Math.floor(first / size) + 1;
        const params = new URLSearchParams({ page, size })
        const controller = new AbortController()
        const signal = controller.signal;

        setLoading(true)

        abortFetch()
        controllerRef.current = controller

        axios
            .get(`/${metadata.read.title}`, { signal, params })
            .then(res => {
                setRows(res.data.items)
                setTotal(res.data.total)
                setLoading(false)
            })
            .catch(err => {
                if (!axios.isCancel(err))
                    throw err
            })
    }

    // form submission
    const handleSubmit = ({ formData }) => {
        axios
            .post(`/${metadata.read.title}`, formData)
            .then(() => fetchData())
    }

    // mount hook
    useEffect(() => {
        $RefParser
            .dereference(metadata.read)
            .then(schema => setReadSchema(schema))
        return abortFetch
    }, [])

    // paging hook
    useEffect(() => {
        fetchData()
    }, [first, size, readSchema]);

    // get datatable columns
    const getColsFromSchema = (schema, prefix = "", result = [])  => {
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

    const handleDelete = ()  => {
        axios
            .delete(`/${metadata.read.title}`, {data: selectedRows.map(row => row._id)})
            .then(() => fetchData())
    }

    // TODO - update
    return (
        <div>
            {metadata.delete && <Button label="Delete" onClick={handleDelete} disabled={!selectedRows.length} />}
            <DataTable
                // data
                value={rows}
                // visual
                scrollable
                scrollHeight="30vh"
                resizableColumns
                columnResizeMode='expand'
                reorderableColumns
                loading={loading}
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
                // selection
                selectionMode='checkbox'
                selection={selectedRows}
                onSelectionChange={(e) => setSelectedRows(e.value)}
            >
                {metadata.delete || metadata.update ?
                    [<Column selectionMode="multiple" headerStyle={{ width: '3rem' }}></Column>].concat(getColsFromSchema(readSchema)) :
                    getColsFromSchema(readSchema)
                }
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
