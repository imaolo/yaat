import { useEffect, useRef, useState, useMemo } from "react"
import { AgGridReact } from "ag-grid-react"
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import {  ModuleRegistry, TextFilterModule, ValidationModule, RowApiModule, CustomFilterModule,
          NumberFilterModule, DateFilterModule, SelectionChangedEvent, IServerSideSelectionState
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

type SingleFilterScalar = {
  type: string
  filter: any
}

type SingleFilterArray = {
  values: string[]
}

export type SingleFilter = {filterType: string} & (SingleFilterScalar | SingleFilterArray)

export type MultiFilter = {
  filterType: 'text' | string
  operator: 'AND' | 'OR'
  conditions: SingleFilter[]
}

type Document = { [key: string]: any };

function generateGroupStages(cols: string[], keys: string[]): Document[] {
  const pipe: Document[] = [];

  if (cols.length === 0 && keys.length === 0) {
    return pipe;
  }

  const lcols = cols.length;
  const d = lcols - keys.length;

  if (d > 0) {
    const lcd = lcols - d;
    const distinctField = cols[lcd];

    pipe.push({
      $group: {
        _id: `$${distinctField}`,
      },
    });

    pipe.push({
      $project: {
        [distinctField]: '$_id',
        ...Object.fromEntries(cols.slice(0, lcd).map((col, i) => [col, keys[i]])),
        group: { $literal: true },
        _id: {
          $function: {
            body: `function() { return new ObjectId(); }`,
            args: [],
            lang: 'js',
          },
        },
      },
    });
  } else if (d === 0) {
    pipe.push({
      $match: Object.fromEntries(cols.map((col, i) => [col, keys[i]])),
    });
  } else {
    throw new Error(`Mismatched keys and columns: ${JSON.stringify({ cols, keys })}`);
  }

  return pipe;
}

function FloatingPanel({ selectedRows, rowCount }: any) {
  const panelRef = useRef<HTMLDivElement>(null);
  const resizeHandleRef = useRef<HTMLDivElement>(null);
  const [isOpen, setIsOpen] = useState(false);
  const [position, setPosition] = useState({ x: 0, y: 80 });
  const [size, setSize] = useState({ width: 300, height: 400 });
  const isDragging = useRef(false);
  const hasMoved = useRef(false);
  const dragStart = useRef({ x: 0, y: 0 });
  const resizeStart = useRef({ x: 0, y: 0, width: 0, height: 0 });

  useEffect(() => {
    const vw = window.innerWidth;
    setPosition({ x: Math.floor((vw - size.width) / 2), y: 80 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleHeaderMouseDown = (e: React.MouseEvent) => {
    dragStart.current = { x: e.clientX, y: e.clientY };
    isDragging.current = true;
    hasMoved.current = false;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const dx = moveEvent.clientX - dragStart.current.x;
      const dy = moveEvent.clientY - dragStart.current.y;
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {
        hasMoved.current = true;
        setPosition((prev) => ({ x: prev.x + dx, y: prev.y + dy }));
        dragStart.current = { x: moveEvent.clientX, y: moveEvent.clientY };
      }
    };

    const onMouseUp = () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      if (!hasMoved.current) setIsOpen((prev) => !prev);
      isDragging.current = false;
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  const handleResizeMouseDown = (e: React.MouseEvent) => {
    e.stopPropagation();
    resizeStart.current = {
      x: e.clientX,
      y: e.clientY,
      width: size.width,
      height: size.height,
    };

    const onMouseMove = (moveEvent: MouseEvent) => {
      const dx = moveEvent.clientX - resizeStart.current.x;
      const dy = moveEvent.clientY - resizeStart.current.y;
      setSize({
        width: Math.max(150, resizeStart.current.width + dx),
        height: Math.max(100, resizeStart.current.height + dy),
      });
    };

    const onMouseUp = () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  return (
    <div
      ref={panelRef}
      style={{
        position: 'fixed',
        top: position.y,
        left: position.x,
        backgroundColor: '#f8f9fa',
        border: '1px solid #ccc',
        borderRadius: '6px',
        boxShadow: '0 2px 10px rgba(0, 0, 0, 0.2)',
        display: 'flex',
        flexDirection: 'column',
        zIndex: 1000,
        userSelect: 'none',
        overflow: 'hidden',
        width: isOpen ? `${size.width}px` : 'auto',
        height: isOpen ? `${size.height}px` : 'auto',
      }}
    >
      <div
        onMouseDown={handleHeaderMouseDown}
        style={{
          padding: '8px 12px',
          cursor: 'move',
          backgroundColor: '#e2e8f0',
          whiteSpace: 'nowrap',
        }}
      >
        📊 Metrics
      </div>

      {isOpen && (
        <div style={{ flex: 1, padding: '1rem' }}>
          <p>selected rows: {selectedRows}</p>
          <p>total rows: {rowCount}</p>
        </div>
      )}

      {isOpen && (
        <div
          ref={resizeHandleRef}
          onMouseDown={handleResizeMouseDown}
          style={{
            width: '16px',
            height: '16px',
            position: 'absolute',
            right: 0,
            bottom: 0,
            cursor: 'nwse-resize',
            background: 'linear-gradient(135deg, #ccc 25%, transparent 25%), linear-gradient(225deg, #ccc 25%, transparent 25%)',
            backgroundSize: '8px 8px',
            backgroundRepeat: 'no-repeat',
            backgroundPosition: 'right bottom',
          }}
        />
      )}
    </div>
  )
}

export type Filter = SingleFilter | MultiFilter

export default function DocTab({ metadata }: Props) {
  const gridRef = useRef<AgGridReact>(null)
  const [gridCols, setGridCols] = useState<ColDef[]>([])
  const [displayForm, setDisplayForm] = useState(false)
  const [selectedRowsCount, setSelectedRowsCount] = useState(0)
  const [rowCount, setRowCount] = useState(0)
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

    if ('selectAll' in e.serverSideState)
      if (e.serverSideState.selectAll)
        return getGridApi().getDisplayedRowCount() - e.serverSideState.toggledNodes.length;
      else
        return e.serverSideState.toggledNodes.length

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
    count: boolean
  }

  async function fetchData ( payload:  any): Promise<any> {
    return (await api.post(`/read/${metadata.read.title}`, payload)).data
  }

  async function fetchData_agg ( payload:  any): Promise<any> {
    return (await api.post(`/read_agg/${metadata.read.title}`, payload)).data
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


const getMongoFilterValue = (filter: SingleFilter): Record<string, any> | string | Date | number => {
  const dataType = filter.filterType
  const value = (filter as SingleFilterScalar).filter ?? (filter as SingleFilterArray).values
  const new_value = convertToDatatype(dataType, value)

    switch (dataType){
      case 'number':
      case 'text':
      case 'set':
        return new_value
      case 'date':
        return { $dateFromString: { dateString: new_value } }
      default:
        throw new Error(dataType)
    }
  }

  // export type TextAdvancedFilterModelType = 'equals' | 'notEqual' | 'contains' | 'notContains' | 'startsWith' | 'endsWith' | 'blank' | 'notBlank';
  // export type ScalarAdvancedFilterModelType = 'equals' | 'notEqual' | 'lessThan' | 'lessThanOrEqual' | 'greaterThan' | 'greaterThanOrEqual' | 'blank' | 'notBlank';
  
  const mongoSingleFilter = (field: string, filter: SingleFilter, not: boolean = false): Record<string, any> => {
    let value = getMongoFilterValue(filter)
    switch (filter.filterType){
      case 'text':
      case 'number':
      case 'date': {
        const scalar_filter = filter as SingleFilterScalar
        switch (filter.filterType) {
          case 'text':
            switch (scalar_filter.type){
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
                throw new Error(scalar_filter.type)
            }
          case 'number':
            switch (scalar_filter.type){
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
                throw new Error(scalar_filter.type)
            }
          case 'date':
            switch (scalar_filter.type){
              case 'after':
                return { $expr : {$gt : [ `$${field}`, value ] } }
              case 'before':
                return { $expr : {$lt : [ `$${field}`, value ] } }
              case 'is':
              case 'isNot':
              case 'between':
                return {[field]: getMongoFilterValue(filter)}
              default:
                throw new Error(scalar_filter.type)
            }
          default:
            throw new Error(filter.filterType)
        }
      }
      case 'set':
        return not ? { [field]: { $nin: value } } : { [field]: { $in: value } }
      default:
        throw new Error(filter.filterType)
    }
  }

  const mongoFilter = (filter: Record<string , Filter>, include_ids: string[] = [], exclude_ids: string[] = []): Record<string, any>  => {
    const new_filter: Record<string, any> = {$and: [{$or: []},]}

    // cannot include both include and exclude ids
    if (include_ids.length > 0 && exclude_ids.length > 0)
      throw new Error(`${include_ids} - ${exclude_ids}`)

    // helpers
    const addOrFilter = (single_filter_field: string, single_filter: SingleFilter) => {
      new_filter.$and[0].$or.push(mongoSingleFilter(single_filter_field, single_filter))
    }
    const addAndFilter = (single_filter_field: string, single_filter: SingleFilter, not:boolean = false) => {
      new_filter.$and.push(mongoSingleFilter(single_filter_field, single_filter, not))
    }

    // main construction logic
    for (const [field, field_filter] of Object.entries(filter))
      if ('operator' in field_filter)
        for (const condition of field_filter.conditions)
          (field_filter.operator === 'AND' ? addAndFilter : addOrFilter)(field, condition)
      else
        addAndFilter(field, field_filter)

    // add include/exclude ids
    if (include_ids.length > 0)
      addAndFilter('_id', { filterType: 'set', values: include_ids})
    if (exclude_ids.length > 0)
      addAndFilter('_id', { filterType: 'set', values: exclude_ids}, true)
  
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
      const payload: any[] = [{$match: mongoFilter(req.filterModel as Record<string, Filter>)}]
      const sort = Object.fromEntries(
        (req.sortModel ?? []).map(({ colId, sort }) => [colId, sort === "asc" ? 1 : -1])
      )
      if (Object.keys(sort).length > 0)
        payload.push({$sort:sort})
      
      // const sort = (req.sortModel ?? {}).map(({ colId, sort }) => [colId, sort === "asc" ? 1 : -1])
      for (const stage of generateGroupStages(req.rowGroupCols.map(group => group.id), req.groupKeys))
        payload.push(stage)
      payload.push({$skip: req.startRow})
      payload.push({$limit: (!req.endRow || !req.startRow) ? 100 : (req.endRow - req.startRow)})

      try {
        // get data
        const data = await fetchData(payload)
        params.success({rowData: data})

        // get count
        payload.pop(); payload.pop()
        payload.push({$count:'count'})
        const { count } = await fetchData_agg(payload)
        if (count !== undefined) {
          params.success({rowCount: count, rowData: data})
          setRowCount(count)
        }
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
    const selectionState = gridapi.getServerSideSelectionState() as IServerSideSelectionState

    if (!selectionState)
      throw new Error()

    let include_ids: string[] = []
    let exclude_ids: string[] = []
    if (selectionState.selectAll) {
      if (selectionState.toggledNodes.length > 0)
        exclude_ids = selectionState.toggledNodes
    }
    else {
      if (!selectionState.toggledNodes)
        throw new Error()
      include_ids = selectionState.toggledNodes
    }
  
    // NOTE um deletes all on selects that arent select all?
    const payload: QueryParams = {
      skip: 0,
      limit: 10**10,
      sort: [],
      filter: mongoFilter(filterModel, include_ids, exclude_ids),
      rowGroupCols: [],
      groupKeys: [],
      count: false
    }

    api
      .post(`/delete/${metadata.read.title}`, payload)
      .then((_) => getGridApi().refreshServerSide())
  }

  const handleSelectionChanged = (e: SelectionChangedEvent) => {
    setSelectedRowsCount(getSelectedRowsCount(e))
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

      <FloatingPanel selectedRows={selectedRowsCount} rowCount={rowCount}/>

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
          onSelectionChanged={handleSelectionChanged}

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
