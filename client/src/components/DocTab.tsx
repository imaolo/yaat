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
import DateTimePopupFilter from "@/components/DateTimePopupFilter"

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

// function DateTimeFilter(props: IFilterParams) {
//   const [value, setValue] = useState<string>("");

//   useEffect(() => {
//     props.filterChangedCallback(); // notify AG Grid on mount
//   }, []);

//   const onChange = (e: React.ChangeEvent<HTMLInputElement>) => {
//     setValue(e.target.value);
//     props.filterChangedCallback(); // tells AG Grid to re-run filtering
//   };

//   // Required by AG Grid
//   const isFilterActive = () => value !== "";

//   // @ts-expect-error TS6133: getModel is used by AG Grid at runtime
//   const doesFilterPass = (params: IDoesFilterPassParams) => {
//     const cellValue = new Date(params.data[props.colDef.field!]).getTime();
//     const filterTime = new Date(value).getTime();
//     return cellValue >= filterTime;
//   };

//   // @ts-expect-error TS6133: getModel is used by AG Grid at runtime
//   const getModel = () => (isFilterActive() ? { value } : null);
//   // @ts-expect-error TS6133: getModel is used by AG Grid at runtime
//   const setModel = (model: any) => {
//     setValue(model?.value ?? "");
//   };

//   return (
//     <div style={{ padding: '4px' }}>
//       <label>After:</label>
//       <input
//         type="datetime-local"
//         value={value}
//         onChange={onChange}
//         style={{ width: '100%' }}
//       />
//     </div>
//   );
// }


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

  // filter: `agNumberColumnFilter`, `agTextColumnFilter`, `agDateColumnFilter`, `agMultiColumnFilter`, `agSetColumnFilter`
  // `'text'`, `'number'`,  `'boolean'`,  `'date'`,  `'dateString'` or  `'object'`,
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
          filter: agt !== 'date' ? true : DateTimePopupFilter,
          cellDataType: agt,
          filterParams : { 
            maxNumConditions: 10,
            buttons: ['apply'],
            closeOnApply: false,
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

  // data source

  const datasource: IServerSideDatasource = {
    getRows: async (params: IServerSideGetRowsParams) => {
      const payload: QueryParams = {
        skip: params.request.startRow,
        limit: (!params.request.endRow || !params.request.startRow) ? 100 : (params.request.endRow - params.request.startRow),
        sort: (params.request.sortModel ?? []).map(({ colId, sort }) => [colId, sort === "asc" ? 1 : -1]),
        filter: {}, // TODO
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
