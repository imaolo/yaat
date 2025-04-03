import { useState, useEffect, useRef } from 'react';
import { DataTable } from 'primereact/datatable';
import { Column }    from 'primereact/column';
import { Button } from 'primereact/button';
import $RefParser from "json-schema-ref-parser";
import axios from 'axios'
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";

export default function DocTab({ metadata }){
    const [rows, setRows] = useState([]);
    const [selectedRows, setSelectedRows] = useState([]);
    const [first, setFirst] = useState(0);
    const [size, setSize] = useState(5);
    const [total, setTotal] = useState(0);
    const [readSchema, setReadSchema] = useState({});
    const [loading, setLoading] = useState(true)
    const [multiSort, setMultiSort] = useState([]);
    const controllerRef = useRef(null);

    // helpers

    const abortFetch = () => {
        if (controllerRef.current)
            controllerRef.current.abort()
    }

    const fetchData = () => {
        const page = Math.floor(first / size) + 1;
        const params = new URLSearchParams({ page, size })
        const controller = new AbortController()
        const signal = controller.signal;

        // setup
        setLoading(true)
        abortFetch()
        controllerRef.current = controller

        // add sorting parameters
        params.append('sort', multiSort.map(item => `${item.field}:${item.order}`).join(','))

        // make request
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

    // handlers

    const handleSubmit = ({ formData }) =>
        axios
            .post(`/${metadata.read.title}`, formData)
            .then(() => fetchData())

    const handleDelete = () =>
        axios
            .delete(`/${metadata.read.title}`, {data: selectedRows.map(row => row._id)})
            .then(() => fetchData())

    // hooks

    useEffect(() => {
        $RefParser
            .dereference(metadata.read)
            .then(schema => setReadSchema(schema))
        return abortFetch
    }, [])

    useEffect(() => fetchData(), [first, size, readSchema, multiSort]);

    // TODO - update
    return (
        <div>
            {metadata.delete && <Button label="Delete" onClick={handleDelete} disabled={!selectedRows.length} />}
            <DataTable
                // data
                value={rows}
                // lazy loading
                lazy
                // TODO filter
                // visual
                scrollable
                scrollHeight="65vh"
                resizableColumns
                columnResizeMode='expand'
                reorderableColumns
                loading={loading}
                // paging
                paginator
                first={first}
                totalRecords={total}
                rows={size}
                onPage={(e) => {
                    setFirst(e.first)
                    setSize(e.rows)
                }}
                rowsPerPageOptions={[5, 25, 50, 100]}
                paginatorTemplate="RowsPerPageDropdown FirstPageLink PrevPageLink CurrentPageReport NextPageLink LastPageLink"
                currentPageReportTemplate="{first} to {last} of {totalRecords}"
                // sorting
                sortMode="multiple"
                multiSortMeta={multiSort}
                onSort={(e) => setMultiSort(e.multiSortMeta)}
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
