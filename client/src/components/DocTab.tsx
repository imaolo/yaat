import { useEffect, useRef, useState, useMemo } from "react"
import { AgGridReact } from "ag-grid-react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import {  ModuleRegistry, TextFilterModule, ValidationModule, RowApiModule, CustomFilterModule,
          NumberFilterModule, DateFilterModule, SelectionChangedEvent
        } from 'ag-grid-community'; 
import { ServerSideRowModelModule, ServerSideRowModelApiModule, PaginationModule, RowGroupingModule, RowGroupingPanelModule, SetFilterModule} from 'ag-grid-enterprise'; 
import Form from "@rjsf/core"
import validator from "@rjsf/validator-ajv8"
import Dereferencer from "@json-schema-tools/dereferencer";
import axios from "axios"
import type { ColDef, IServerSideDatasource, IServerSideGetRowsParams} from "ag-grid-community"
import type { IChangeEvent } from "@rjsf/core"
import type { Metadata } from "@/App"
import DateTimeFilterPopup from "@/components/DateTimeFilterPopup"

ModuleRegistry.registerModules([
  ServerSideRowModelModule,
  ServerSideRowModelApiModule,
  RowApiModule,
  PaginationModule,
  TextFilterModule,
  NumberFilterModule,
  DateFilterModule,
  CustomFilterModule,
  ValidationModule,
  RowGroupingModule,
  RowGroupingPanelModule,
  SetFilterModule
]); 

type Props = {
  metadata: Metadata
}

export type SingleFilter = {
  filterType: 'text' | string;
  type: string
  filter: any
}

export type MultiFilter = {
  filterType: 'text' | string
  operator: 'AND' | 'OR'
  conditions: SingleFilter[]
}

export type Filter = SingleFilter | MultiFilter

export default function DocTab({ metadata }: Props) {
  const gridRef = useRef<AgGridReact>(null)
  const [gridCols, setGridCols] = useState<ColDef[]>([])
  const [displayForm, setDisplayForm] = useState(false)
  const [selectedRowsCount, setSelectedRowsCount] = useState(0)
  const api = axios.create()

  // mount hook

  useEffect(() => {
    const loadSchema = async () => {
        const new_schema = await (new Dereferencer(metadata.read)).resolve()
        setGridCols(getColsFromSchema(new_schema))
    }
    loadSchema()
  }, [])

  // helpers

  const getObjVal = (obj: Record<string, any>, path: string): any => path.split('.').reduce((acc, key) => acc?.[key], obj)

  const jsonSchema2AGT = (schema: any): 'date' | 'number' | 'text' => {
    if (schema.type === 'string' && schema.format === 'date-time')
      return 'date'

    switch (schema.type){
      case 'number':
      case 'integer': return 'number'
      case 'string': return 'text'
      default: return 'text'
    }
  }

  const jsonSchema2AgFilter = (schema: any): any => {
    if (schema.type === 'string' && schema.format === 'date-time')
      return DateTimeFilterPopup

    if ('enum' in schema)
      return 'agSetColumnFilter'

    switch (schema.type){
      case 'number':
      case 'integer':
        return 'agNumberColumnFilter'
      case 'string':
        return 'agTextColumnFilter'
    }
  }

  const getColsFromSchema = (schema: any, prefix = "", result: ColDef[] = []): ColDef[] => {
    if (!schema?.properties) return result
    for (const [key, propSchema] of Object.entries(schema.properties) as [string, any][]) {
      const path = prefix ? `${prefix}.${key}` : key
      if (propSchema.type === "object" && propSchema.properties)
        getColsFromSchema(propSchema, path, result)
      else {
        let agt = jsonSchema2AGT(propSchema)
        result.push({
          field: path,
          headerName: path,
          sortable: true,
          enableRowGroup: true,
          filter: jsonSchema2AgFilter(propSchema),
          cellDataType: agt,
          filterParams : { 
            maxNumConditions: 10,
            buttons: ['apply'],
            closeOnApply: true,
            key: `${Date.now()}`,
            ...('enum' in propSchema && { values: propSchema['enum'] })
          },
          flex: 1,
          minWidth: 100,
          valueGetter: (params) => {
            let val = getObjVal(params.data, path)
            if (agt !== 'date' ||  !val)
              return val
            else {
              let d = new Date(val)
              if (isNaN(d.getTime()))
                throw Error(val)
              return d
            }
          },
          valueFormatter: (params) => {
            let val = getObjVal(params.data, path)
            if (agt !== 'date' || !val)
              return val
            else {
              const d = new Date(val);
              if (isNaN(d.getTime())) throw Error(val);
              return d.toLocaleString(undefined, {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: 'numeric',
                minute: '2-digit',
                hour12: true,
              });
            }
          }
        })
      }
    }
    return result
  }

  const getGridApi = () => {
    const gridapi = gridRef.current?.api
    if (!gridapi)
      throw Error()
    return gridapi
  }

  const getSelectedRowsCount = (e: SelectionChangedEvent): number => {
    if (!e.serverSideState)
      throw Error()

    if (e.selectedNodes && e.selectedNodes.length > 1)
      return e.selectedNodes.length

    if ('selectAll' in e.serverSideState){
      if (e.serverSideState.selectAll)
        return getGridApi().getDisplayedRowCount();
      else{
        if (e.serverSideState.toggledNodes.length > 0)
          throw new Error()
        return 0
      }
    }

    throw Error()
  }

  const getRowId = (params: { data: any }) => params.data._id

  interface QueryParams {
    skip: any | number;
    limit: any | number;
    sort: any | [string, 1 | -1][];
    filter: any | Record<string, unknown>;
    rowGroupCols: string[]
    groupKeys: string[]
  }

  async function fetchData ( payload:  QueryParams) {
    const { data } = await api.post(`/read/${metadata.read.title}`, payload);
    return {
      rowData: data.items,
      rowCount: data.total
    }
  }

  const convertToDatatype = (dataType: string, value: string): string | Date | Number => {
    const type = (dataType || 'text').toLowerCase();

    switch (type) {
        case 'numeric':
          const numericValue = Number(value)
          if (isNaN(numericValue))
            throw new Error(`Invalid numeric value: ${value}`);
          return numericValue;
        case 'date':
          const dateValue = new Date(value[0]);
          if (isNaN(dateValue.getTime()))
            throw new Error(`Invalid date value: ${value[0]}`);
          return dateValue
        case 'text':
        default:
          return value;
    }
};


  const getMongoFilterValue = (dataType: string, value: string): Record<string, any> | string | Date | number => {
    const new_value = convertToDatatype(dataType, value)
  
    switch (dataType){
      case 'number':
      case 'text':
        return new_value
      case 'date':
        return { $dateFromString: { dateString: new_value } }
      case 'set':
        console.log(new_value)
        throw new Error(`${new_value}`)
      default:
        throw new Error(dataType)
    }
  }

  // export type TextAdvancedFilterModelType = 'equals' | 'notEqual' | 'contains' | 'notContains' | 'startsWith' | 'endsWith' | 'blank' | 'notBlank';
  // export type ScalarAdvancedFilterModelType = 'equals' | 'notEqual' | 'lessThan' | 'lessThanOrEqual' | 'greaterThan' | 'greaterThanOrEqual' | 'blank' | 'notBlank';
  
  const mongoSingleFilter = (field: string, filter: SingleFilter): Record<string, any> => {
    let value = getMongoFilterValue(filter.filterType, filter.filter)
    switch (filter.filterType){
      case 'text':
        switch (filter.type){
          case 'equals':
            return {[field]: value}
          case 'notEqual': 
            return { [field]: { $ne:  value } }
          case 'contains':
            return { [field]: { $regex: value } }
          case 'notContains':
            return { [field]: { $not: { $regex: value } } }
          case 'startsWith':
            return { [field]: { $regex: `^${value}` } }
          case 'endsWith':
            return { [field]: { $regex: `${value}$` } }
          case 'blank':
          case 'notBlank':
          default:
            throw new Error(filter.type)
        }
      case 'number':
        switch (filter.type){
          case 'equals':
            return {[field]: value}
          case 'notEqual': 
            return { [field]: { $ne:  value } }
          case 'lessThan':
            return { [field]: { $lt: value } }
          case 'lessThanOrEqual':
            return { [field]: { $lte: value } }
          case 'greaterThan':
            return { [field]: { $gt: value } }
          case 'greaterThanOrEqual':
            return { [field]: { $gte: value } }
          case 'blank':
          case 'notBlank':
          default:
            throw new Error(filter.type)
        }
      case 'date':
        switch (filter.type){
          case 'after':
            return { $expr : {$gt : [ `$${field}`, value ] } }
          case 'before':
            return { $expr : {$lt : [ `$${field}`, value ] } }
          case 'is':
          case 'isNot':
          case 'between':
            return {[field]: getMongoFilterValue(filter.filterType, filter.filter)}
          default:
            throw new Error(filter.type)
        }
      case 'set':
        return { [field]: { $in: value } }
      default:
        throw new Error(filter.filterType)
    }
  }

  const mongoFilter = (filter: Record<string , Filter>): Record<string, any>  => {
    const new_filter: Record<string, any> = {$and: [{$or: []},]}

    // helpers
    const addOrFilter = (single_filter_field: string, single_filter: SingleFilter) => {
      new_filter.$and[0].$or.push(mongoSingleFilter(single_filter_field, single_filter))
    }
    const addAndFilter = (single_filter_field: string, single_filter: SingleFilter) => {
      new_filter.$and.push(mongoSingleFilter(single_filter_field, single_filter))
    }

    // main construction logic
    for (const [field, field_filter] of Object.entries(filter))
      if ('operator' in field_filter)
        for (const condition of field_filter.conditions)
          (field_filter.operator === 'AND' ? addAndFilter : addOrFilter)(field, condition)
      else
        addAndFilter(field, field_filter)
  
    // pop OR if empty
    if (new_filter.$and[0].$or.length === 0)
      new_filter.$and.shift()

    // pop and if empty
    return new_filter.$and.length > 0 ? new_filter : {}
  }

  // data source

  const datasource = useMemo<IServerSideDatasource>(() => ({
    getRows: async (params: IServerSideGetRowsParams) => {
      const req = params.request
      const payload: QueryParams = {
        skip: req.startRow,
        limit: (!req.endRow || !req.startRow) ? 100 : (req.endRow - req.startRow),
        sort: (req.sortModel ?? []).map(({ colId, sort }) => [colId, sort === "asc" ? 1 : -1]),
        filter: mongoFilter(req.filterModel as Record<string, Filter>),
        rowGroupCols: req.rowGroupCols.map(group => group.id),
        groupKeys: req.groupKeys
      }
  
      try {
        params.success(await fetchData(payload))
      } catch (e) {
        params.fail()
      }
    }
  }), [metadata])

  // event handlers

  const handleSubmit = (data: IChangeEvent<any>, _: React.FormEvent) => {
    api.post(`/${metadata.read.title}`, data.formData).then(() => getGridApi().refreshServerSide())
  }

  const handleDelete = () => {
    const gridapi = getGridApi()
    const filterModel = gridapi.getFilterModel() as Record<string, Filter>
  
    // NOTE um deletes all on selects that arent select all?
    const payload: QueryParams = {
      skip: 0,
      limit: 10**10,
      sort: [],
      filter: mongoFilter(filterModel),
      rowGroupCols: [],
      groupKeys: [],
    }

    api
      .post(`/delete/${metadata.read.title}`, payload)
      .then((_) => getGridApi().refreshServerSide())
  }

  // DocTab component

  return (
    <div className="flex flex-col gap-2 w-full h-[75vh]">
      <div className="flex items-center gap-2 p-2">
        <Button onClick={() => getGridApi().refreshServerSide()}>Refresh</Button>
        {metadata.create && <Button onClick={() => setDisplayForm(true)}>Create</Button>}
        {metadata.delete && <Button onClick={handleDelete} disabled={selectedRowsCount <= 0}>Delete</Button>}
        {metadata.update && <Button disabled={selectedRowsCount != 1}>Update</Button>}
        <span className="ml-auto text-sm">Rows Selected: {selectedRowsCount}</span>
      </div>

      <div className="ag-theme-alpine w-full h-full border border-gray-600 rounded">
        <AgGridReact
          // basic
          ref={gridRef}
          columnDefs={gridCols}
          serverSideDatasource={datasource}

          // row model
          rowModelType={'serverSide'}
          getRowId={getRowId}

          // selection
          rowSelection={{
            mode: 'multiRow',
            enableClickSelection: false,
          }}
          onSelectionChanged={(e: SelectionChangedEvent) => setSelectedRowsCount(getSelectedRowsCount(e))}

          // visual
          animateRows={false}

          // pagination and memory management
          pagination={true}
          paginationPageSizeSelector={[20, 50, 10**2, 10**3]}
          paginationPageSize={20}
          cacheBlockSize={25}
          maxBlocksInCache={10000*25}

          // sort
          multiSortKey='ctrl'

          // // grouping
          rowGroupPanelShow="always"
          // rowGroupPanelShow="always"
          // // enableRowGroupPanel={true}
          // // enableRowGroup={true}
        />
      </div>

      <Dialog open={displayForm} onOpenChange={setDisplayForm}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Create New Record</DialogTitle>
          </DialogHeader>
          <Form schema={metadata.create} validator={validator} onSubmit={handleSubmit} />
        </DialogContent>
      </Dialog>
    </div>
  )
}
