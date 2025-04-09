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
    let col2type = {}

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

    const getJsonQuery = (field, matchOp, str_value) => {
        const value = convertToDatatype(col2type[field], str_value)
        switch (matchOp){
            // all type match modes
            case FilterMatchMode.EQUALS:
                return { [field]: value }
            case FilterMatchMode.NOT_EQUALS:
                return { [field]: { $ne: value } }
            // string match modes
            case FilterMatchMode.STARTS_WITH:
                return { [field]: { $regex: `^${value}` } }
            case FilterMatchMode.CONTAINS:
                return { [field]: { $regex: value } }
            case FilterMatchMode.NOT_CONTAINS:
                return { [field]: { $not: { $regex: value } } }
            case FilterMatchMode.ENDS_WITH:
                return { [field]: { $regex: `${value}$` } }
            // numeric match modes
            case FilterMatchMode.LESS_THAN:
                return { [field]: { $lt: value } }
            case FilterMatchMode.LESS_THAN_OR_EQUAL_TO:
                return { [field]: { $lte: value } }
            case FilterMatchMode.GREATER_THAN:
                return { [field]: { $gt: value } }
            case FilterMatchMode.GREATER_THAN_OR_EQUAL_TO:
                return { [field]: { $gte: value } }
            // date match modes
            case FilterMatchMode.DATE_IS:
                return { $expr : {$eq : [ `$${field}`, { $dateFromString: { dateString: value } } ] } }
            case FilterMatchMode.DATE_IS_NOT:
                return { $expr : {$ne : [ `$${field}`, { $dateFromString: { dateString: value } } ] } }
            case FilterMatchMode.DATE_BEFORE:
                return { $expr : {$lt : [ `$${field}`, { $dateFromString: { dateString: value } } ] } }
            case FilterMatchMode.DATE_AFTER:
                return { $expr : {$gt : [ `$${field}`, { $dateFromString: { dateString: value } } ] } }
            default:
                throw new Error(`invalid match mode op ${matchOp}`)
        }
    }

    const fetchData = () => {
        // extract ands and ors
        let filter_ors = []
        let filter_ands = []
        for (const [field, field_filter] of Object.entries(tableFilters))
            for (const constraint of field_filter.constraints)
                if (constraint.value)
                    if (field_filter.operator === FilterOperator.AND)
                        filter_ands.push([field, constraint])
                    else if (field_filter.operator === FilterOperator.OR)
                        filter_ors.push([field, constraint])
                    else
                        throw new Error(`invalid filter operator ${field.filter.operator}`)

        // create the filter
        const filter = {$and: [{$or: []},]}
        for (const filter_or of filter_ors)
            filter.$and[0].$or.push(getJsonQuery(filter_or[0], filter_or[1].matchMode, filter_or[1].value))
        for (const filter_and of filter_ands)
            filter.$and.push(getJsonQuery(filter_and[0], filter_and[1].matchMode, filter_and[1].value))

        // pop $or if there is nothing
        if (filter.$and[0].$or.length === 0)
            filter.$and.shift()

        // construct the payload
        const payload = {
            filter: filter.$and.length === 0 ? {} : filter,
            sort: multiSort.map(cur => [cur.field, cur.order]),
            skip: first,
            limit: size
        }
        
        // abort previous request
        abortFetch()

        // create new abort controller
        const controller = new AbortController();
        controllerRef.current = controller;

        // make request
        api
            .post(`/read/${metadata.read.title}`, payload, { signal:  controller.signal})
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

    const convertToDatatype = (dataType, value) => {
        // Default to 'text' if no dataType is provided.
        const type = (dataType || 'text').toLowerCase();
    
        switch (type) {
            case 'numeric':
                const numericValue = Number(value)
                if (isNaN(numericValue))
                    throw new Error(`Invalid numeric value: ${value}`);
                return numericValue;
            case 'date':
                const dateValue = new Date(value);
                // Check if the conversion produced a valid date.
                if (isNaN(dateValue.getTime()))
                    throw new Error(`Invalid date value: ${value}`);
                return dateValue
            case 'text':
            default:
                return value;
        }
    };

    const getColsFromSchema = (schema, prefix = "", result = [])  => {
        if (!schema || !schema.properties) return result;
    
        for (const [propName, propSchema] of Object.entries(schema.properties)) {
            const fullPath = prefix ? `${prefix}.${propName}` : propName;
            if (propSchema.type === "object" && propSchema.properties)
                getColsFromSchema(propSchema, fullPath, result);
            else {
                const prime_t = jsonSchema2PrimeType(propSchema)
                col2type[fullPath] = prime_t
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
                {
                    <Button
                        label="Refresh"
                        icon="pi pi-refresh"
                        onClick={() => {
                            setMultiSort([])
                            setTableFilters(initFiltersFromSchema(readSchema))
                            fetchData()
                        }}/>
                }
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
