import { useEffect, useRef, useState } from "react"
import { AgGridReact } from "ag-grid-react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import {  ModuleRegistry, TextFilterModule, ValidationModule, RowApiModule, CustomFilterModule,
          NumberFilterModule, DateFilterModule
        } from 'ag-grid-community'; 
import { ServerSideRowModelModule, ServerSideRowModelApiModule, PaginationModule } from 'ag-grid-enterprise'; 
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
  ValidationModule
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
  const [rowsSelected, setRowsSelected] = useState(0)
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

  const jsonSchema2AGT = (schema: any) => {
    if (schema.type === 'string' && schema.format === 'date-time')
      return 'date'

    switch (schema.type){
      case 'number':
      case 'integer': return 'number'
      case 'string': return 'text'
      default: return 'text'
    }
  }

  const getColsFromSchema = (schema: any, prefix = "", result: ColDef[] = []) => {
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
          filter: agt !== 'date' ? true : DateTimeFilterPopup,
          cellDataType: agt,
          filterParams : { 
            maxNumConditions: 10,
            buttons: ['apply'],
            closeOnApply: true,
            key: `${Date.now()}`
          },
          flex: 1,
          minWidth: 100,
          valueGetter: (params) => {
            let val = getObjVal(params.data, path)
            if (agt !== 'date')
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
            if (agt !== 'date')
              return val
            else {
              let d = new Date(val)
              if (isNaN(d.getTime()))
                throw Error(val)
              return d.toISOString()
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

  const getCurrentPageRows = () => {
    const gridapi = getGridApi()
    const size   = gridapi.paginationGetPageSize();
    const index  = gridapi.paginationGetCurrentPage();
    const total  = gridapi.getDisplayedRowCount();
  
    const rows: any[] = [];
    const start = index * size;
    const end   = Math.min(start + size, total);
  
    for (let i = start; i < end; i++)
      rows.push(gridapi.getDisplayedRowAtIndex(i)!.data);
    return rows;
  }

  const getSelectedRows = () => {
    const state = getGridApi().getServerSideSelectionState()
    if (!state)
      throw Error()

    if (state.toggledNodes && state.toggledNodes.length > 1)
      return state.toggledNodes
    else if ('selectAll' in state)
      return state.selectAll ? getCurrentPageRows() : []
  
    throw Error()
  }

  const getRowId = (params: { data: any }) => params.data._id

  interface QueryParams {
    skip: any | number;
    limit: any | number;
    sort: any | [string, 1 | -1][];
    filter: any | Record<string, unknown>;
  }

  async function fetchData ( payload:  QueryParams) {
    const { data } = await api.post(`/read/${metadata.read.title}`, payload);
    return {
      rowData: data.items,
      rowCount: data.total
    }
  }

  const mongoConvert = (trg_t: string, value: string): any => {
    switch (trg_t){
      case 'text': return value
      case 'date': return {'$dateFromString': value}
      case 'number': return {'$numberFromString': value}
      default: throw Error(`${trg_t} -  ${value}`)
    }
  }

  const mongoSingleFilter = (filter: SingleFilter): Record<string, any> => {
      switch (filter.filterType){
        case 'text': return mongoConvert(filter.filterType, filter.filter)
        case 'date': return mongoConvert(filter.filterType, filter.filter)
        case 'number': return mongoConvert(filter.filterType, filter.filter)
        default: throw Error(filter.filterType)
      }

      // TODO - handle type
  }

  const mongoFilter = (filter: Record<string , Filter>): Record<string, any>  => {
    console.log(filter)
    const new_filter: Record<string, any> = {$and: [{$or: []},]}

    // TODO implement these

    const addOrFilter = (single_filter_field: string, single_filter: SingleFilter) => {
      new_filter.$and[0].$or.push({[single_filter_field] : mongoSingleFilter(single_filter)})
    }

    const addAndFilter = (single_filter_field: string, single_filter: SingleFilter) => {
      new_filter.$and.push({[single_filter_field] : mongoSingleFilter(single_filter)})
    }

    // main construction logic

    for (const [field, field_filter] of Object.entries(filter))
      if ('operator' in field_filter)
        for (const condition of field_filter.conditions)
          (field_filter.operator === 'AND' ? addAndFilter : addOrFilter)(field, condition)
      else
        addAndFilter(field, field_filter)

    return {}
  }

  // data source

  const datasource: IServerSideDatasource = {
    getRows: async (params: IServerSideGetRowsParams) => {
      const payload: QueryParams = {
        skip: params.request.startRow,
        limit: (!params.request.endRow || !params.request.startRow) ? 100 : (params.request.endRow - params.request.startRow),
        sort: (params.request.sortModel ?? []).map(({ colId, sort }) => [colId, sort === "asc" ? 1 : -1]),
        filter: mongoFilter(params.request.filterModel as Record<string, Filter>)
      }

      try {
        params.success(await fetchData(payload))
      } catch (e) {
        params.fail()
      }
    },
  }

  // event handlers

  const handleSubmit = (data: IChangeEvent<any>, _: React.FormEvent) => {
    api.post(`/${metadata.read.title}`, data.formData).then(() => getGridApi().refreshServerSide())
  }

  const handleDelete = () => {
    const gridapi = getGridApi()
    const currentPage = gridapi.paginationGetCurrentPage();
    const pageSize = gridapi.paginationGetPageSize();
    const skip = currentPage * pageSize;
  
    const payload: QueryParams = {
      skip,
      limit: pageSize,
      sort: [],
      filter: {}, // TODO
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
        {metadata.delete && <Button onClick={handleDelete} disabled={rowsSelected <= 0}>Delete</Button>}
        {metadata.update && <Button disabled={rowsSelected != 1}>Update</Button>}
        <span className="ml-auto text-sm">Rows Selected: {rowsSelected}</span>
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
          onSelectionChanged={() => setRowsSelected(getSelectedRows().length)}

          // visual
          animateRows={true}

          // pagination and memory management
          pagination={true}
          paginationPageSizeSelector={[20, 50, 100, 1000, 10000]}
          paginationPageSize={20}
          cacheBlockSize={25}
          maxBlocksInCache={10000*25}

          // sort
          multiSortKey='ctrl'
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
