import { useState, useEffect, useRef, useTransition } from 'react';
import { DataTable } from 'primereact/datatable';
import { Column }    from 'primereact/column';
import { Button } from 'primereact/button';
import { Dialog } from 'primereact/dialog';
import { FilterMatchMode, FilterOperator } from 'primereact/api';
import { ProgressSpinner } from 'primereact/progressspinner';
import { Calendar } from 'primereact/calendar';
import $RefParser from "json-schema-ref-parser";
import axios from 'axios'
import Form from "@rjsf/core";
import { BlockUI } from 'primereact/blockui';
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
    const [tableFilters, setTableFilters] = useState({});
    const [displayForm, setDisplayForm] = useState(false)
    const [isPending, startTransition] = useTransition();
    const [selectAll, setSelectAll] = useState(false);
    const [selectedRowsLength, setSelectedRowsLength] = useState(0);
    const controllerRef = useRef(null);
    const api = axios.create();

    // configure api interceptors

    api.interceptors.request.use(
        (config) => {
            setLoading(true)
            return config
        },
        (error) => {
            setLoading(false)
            return Promise.reject(error)
        }
    )

    api.interceptors.response.use(
        (response) => {
            setLoading(false)
            return response;
        },
        (error) => {
            setLoading(false)
            return Promise.reject(error);
        }
    );

    // helpers

    const abortFetch = () => {
        if (controllerRef.current)
            controllerRef.current.abort()
    }

    const fetchData = () => {
        const params = new URLSearchParams()
        const controller = new AbortController()
        const signal = controller.signal;

        // TODO paging and filters happening in mongo query language now
        // params.set('$skip', first)
        // params.set('$top', size)
        // if (multiSort && multiSort.length)
        //     params.set('$orderby', multiSort
        //         .map(item => `${item.field} ${item.order === 1 ? 'asc' : 'desc'}`)
        //         .join(','));
        
        // setup
        abortFetch()
        controllerRef.current = controller

        // make request
        api
            .get(`/${metadata.read.title}`, { signal, params })
            .then(res => {
                process
                setRows(res.data.items)
                setTotal(res.data.total)
                setSelectAll(false)
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
            else {
                const prime_t = jsonSchema2PrimeType(propSchema)
                result.push(
                    <Column
                        field={fullPath}
                        header={fullPath}
                        style={{ width: "15%", minWidth: "20px" }}
                        sortable 
                        filter
                        filterElement={primeType2FilterElement(prime_t)}
                        dataType={prime_t}
                    />
                )
            }
        }
        return result
    }

    const initFiltersFromSchema = (schema, prefix = "", result = {}) => {
        if (!schema || !schema.properties) return result;
      
        for (const [propName, propSchema] of Object.entries(schema.properties)) {
            const fullPath = prefix ? `${prefix}.${propName}` : propName;
            if (propSchema.type === "object" && propSchema.properties)
                initFiltersFromSchema(propSchema, fullPath, result);
            else
                result[fullPath] = {
                    operator: FilterOperator.AND,
                    constraints: [
                        { value: null, matchMode: primeType2DefaultMatchMode[jsonSchema2PrimeType(propSchema)] }
                    ]
                }
        }
        return result;
    }

    const dateFilterTemplate = (options) => {
        return <Calendar
            value={options.value}
            onChange={(e) => options.filterCallback(e.value, options.index)}
            dateFormat="mm/dd/yy HH:mm"
            showTime
            showTimeIcon="pi pi-clock"
            placeholder="mm/dd/yy HH:mm"
            mask="99/99/9999 99:99"
        />;
    };
    
    const jsonSchema2PrimeType = (schema) => {
        if (!schema)
            return null
        return (schema.type === 'string' && schema.format === 'date-time') ? "date" : {
            string: "text",
            number: "numeric",
            integer: "numeric"
        }[schema.type]
    }
    
    const primeType2DefaultMatchMode = {
        text: FilterMatchMode.CONTAINS,
        date: FilterMatchMode.DATE_IS,
        numeric: FilterMatchMode.EQUALS
    }
    
    const primeType2FilterElement = (prime_t) =>prime_t === "date" ? dateFilterTemplate : null

    // handlers

    const handleSubmit = ({ formData }) => 
        api
            .post(`/${metadata.read.title}`, formData)
            .then(() => fetchData())

    const handleDelete = () => {
        if (selectAll)
            api
                .delete(`/${metadata.read.title}_all`)
                .then(() => fetchData())
        else
            api
                .delete(`/${metadata.read.title}`, {data: selectedRows.map(row => row._id)})
                .then(() => fetchData())
    }

    // hooks

    useEffect(() => {
        $RefParser
            .dereference(metadata.read)
            .then(schema => {
                setReadSchema(schema)
                console.log(initFiltersFromSchema(schema))
                setTableFilters(initFiltersFromSchema(schema))
            })
        return abortFetch
    }, [])

    useEffect(() => fetchData(), [first, size, readSchema, multiSort, tableFilters])

    useEffect(() => {
        setSelectedRows(selectAll ? rows: [])
        setSelectedRowsLength(selectAll ? total : 0)
    }, [selectAll])

    // crud buttons and display number of rows selected

    const header = (
        <div>
            <div style={{ display: "flex", gap: "1rem", minHeight: "3rem" } }>
                {<Button label="Refresh" icon="pi pi-refresh" onClick={fetchData}/>}
                {metadata.delete && <Button label="Delete" onClick={handleDelete} disabled={selectedRowsLength < 1} />}
                {metadata.create && <Button label="Create" icon="pi pi-plus" onClick={() => setDisplayForm(true)} />}
                {metadata.update && <Button label="Update" onClick={() => {}}  disabled={selectedRowsLength != 1}/>}
            </div>
            <div style={{ display: "flex", gap: "1rem", minHeight: "3rem", padding: "5px"} }><p1>Rows Selected: {selectedRowsLength}</p1></div>
        </div>
    )

    // create and return the DocTab component

    return (
        <BlockUI
            blocked={isPending}
            template={<ProgressSpinner style={{width: '50px', height: '50px'}} strokeWidth="8" />}
        >
            <div>
                <div style={{ height: "90%", width: "90%", margin: "0 auto", overflowX: "auto", border: "2px solid currentColor"}}>
                    <DataTable
                        // header
                        header={header}
                        // efficiency
                        virtualScroll
                        // data
                        value={rows}
                        dataKey='_id'
                        // lazy loading
                        lazy    
                        // filter
                        onFilter={(e) => setTableFilters(e.filters)}
                        filters={tableFilters}
                        // visual
                        scrollable
                        scrollHeight="65vh"
                        resizableColumns
                        // tableStyle={{ tableLayout: 'fixed', width: '100%' }} // force fixed layout
                        // columnResizeMode='fit'
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
                        onSelectionChange={(e) => startTransition(() => {
                            setSelectedRows(e.value)
                            setSelectedRowsLength(e.value.length)
                        })}
                        onSelectAllChange={(e) => setSelectAll(e.checked)}
                        selectAll={selectAll}
                    >
                        {metadata.delete || metadata.update ?
                            [<Column
                                frozen
                                selectionMode="multiple"
                                style={{ width: "1%", minWidth: "50px"}}
                            />].concat(getColsFromSchema(readSchema)) :
                            getColsFromSchema(readSchema)
                        }
                    </DataTable>
                </div>
                <Dialog 
                    header="Create New Record" 
                    visible={displayForm} 
                    style={{ width: '50vw' }} 
                    modal 
                    onHide={() => setDisplayForm(false)}
                >
                    <Form
                        schema={metadata.create}
                        validator={validator}
                        onSubmit={handleSubmit}
                    />
                </Dialog>
            </div>
        </BlockUI>
    );
}



// class FilterOptions {
//     constructor({ options, buildFunc }) {
//         this.options = options;
//         this.buildFunc = buildFunc;
//     }

//     generate(field, value) {
//         return this.buildFunc(field, value);
//     }
// }

// const jsonTypeFilterMongoMap = {
//     string: [
//         new FilterOptions({
//             options: { label: "equals", value: FilterMatchMode.EQUALS },
//             buildFunc: (field, value) => ({ [field]: { $eq: value } })
//         }),
//         new FilterOptions({
//             options: { label: "not equals", value: FilterMatchMode.NOT_EQUALS },
//             buildFunc: (field, value) => ({ [field]: { $ne: value } })
//         }),
//         new FilterOptions({
//             options: { label: "contains", value: FilterMatchMode.CONTAINS },
//             buildFunc: (field, value) => ({ [field]: { $regex: `.*${value}.*`, $options: "i" } })
//         }),
//         new FilterOptions({
//             options: { label: "not contains", value: FilterMatchMode.NOT_CONTAINS },
//             buildFunc: (field, value) => ({ [field]: { $not: { $regex: `.*${value}.*`, $options: "i" } } })
//         }),
//         new FilterOptions({
//             options: { label: "starts with", value: FilterMatchMode.STARTS_WITH },
//             buildFunc: (field, value) => ({ [field]: { $regex: `^${value}`, $options: "i" } })
//         }),
//         new FilterOptions({
//             options: { label: "ends with", value: FilterMatchMode.ENDS_WITH },
//             buildFunc: (field, value) => ({ [field]: { $regex: `${value}$`, $options: "i" } })
//         }),
//     ],
//     number: [
//         new FilterOptions({
//             options: { label: "equals", value: FilterMatchMode.EQUALS },
//             buildFunc: (field, value) => ({ [field]: { $eq: Number(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "not equals", value: FilterMatchMode.NOT_EQUALS },
//             buildFunc: (field, value) => ({ [field]: { $ne: Number(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "greater than", value: FilterMatchMode.GREATER_THAN },
//             buildFunc: (field, value) => ({ [field]: { $gt: Number(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "greater or equal", value: FilterMatchMode.GREATER_THAN_OR_EQUAL },
//             buildFunc: (field, value) => ({ [field]: { $gte: Number(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "less than", value: FilterMatchMode.LESS_THAN },
//             buildFunc: (field, value) => ({ [field]: { $lt: Number(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "less or equal", value: FilterMatchMode.LESS_THAN_OR_EQUAL },
//             buildFunc: (field, value) => ({ [field]: { $lte: Number(value) } })
//         }),
//     ],
//     date: [
//         new FilterOptions({
//             options: { label: "equals", value: FilterMatchMode.EQUALS },
//             buildFunc: (field, value) => ({ [field]: { $eq: new Date(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "before", value: FilterMatchMode.LESS_THAN },
//             buildFunc: (field, value) => ({ [field]: { $lt: new Date(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "on or before", value: FilterMatchMode.LESS_THAN_OR_EQUAL },
//             buildFunc: (field, value) => ({ [field]: { $lte: new Date(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "after", value: FilterMatchMode.GREATER_THAN },
//             buildFunc: (field, value) => ({ [field]: { $gt: new Date(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "on or after", value: FilterMatchMode.GREATER_THAN_OR_EQUAL },
//             buildFunc: (field, value) => ({ [field]: { $gte: new Date(value) } })
//         }),
//         new FilterOptions({
//             options: { label: "between", value: "BETWEEN" },
//             buildFunc: (field, valueArray) => ({
//                 [field]: { $gte: new Date(valueArray[0]), $lte: new Date(valueArray[1]) }
//             })
//         }),
//     ]
// };

// const dateBodyTemplate = (rowData) => {
//     return formatDate(rowData.date);
// };


    // const appendFiltersToParams = (params, filters) => {
    //     // NOTE: This is a basic conversion.
    //     // For more complex logic (like multiple constraints or different matchModes) additional work will be needed.
    //     for (const f in filters) {
    //         const filterMeta = filters[f];
    //         if (filterMeta && filterMeta.value != null && filterMeta.value !== '') {
    //             // Append filter as field=<value> or field__matchMode=<mode>
    //             params.append(f, filterMeta.value);
    //             // Optionally, send match mode if needed. Here we assume 'contains' for strings and 'equals' for others.
    //             params.append(`${f}__matchMode`, filterMeta.matchMode || (typeof filterMeta.value === "number" ? "equals" : "contains"));
    //         }
    //     }
    // }

