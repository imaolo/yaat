import { forwardRef, useRef, useState, useEffect } from "react";
import type { IDoesFilterPassParams } from "ag-grid-community";
import type { CustomFilterProps } from "ag-grid-react";
import { useGridFilter } from "ag-grid-react";
import { Button } from "@/components/ui/button";
import DateTimePickerPopup from "@/components/DateTimePickerPopup";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const DATE_TIME_OPTIONS = ['equals', 'notEqual', 'greaterThan', 'lessThan', 'inRange'] as const;
type DateTimeModelType = typeof DATE_TIME_OPTIONS[number];

const LABELS: Record<DateTimeModelType, string> = {
  equals: 'Equals',
  notEqual: 'Not Equal',
  greaterThan: 'After',
  lessThan: 'Before',
  inRange: 'In Range',
};

type SingleFilter = {
  type: DateTimeModelType;
  dateFrom: string;
  dateTo: string;
};

type DateTimeModel = {
  op: 'AND' | 'OR';
  filters: SingleFilter[];
};

const DateTimeFilterPopup = forwardRef<unknown, CustomFilterProps<any, any, DateTimeModel>>(
  ({ model, onModelChange, api}, _ref) => {
    const initialFilters: SingleFilter[] = model?.filters?.length
      ? model.filters
      : [{ type: 'greaterThan', dateFrom: '', dateTo: '' }];

    const [op, setOp] = useState<'AND' | 'OR'>(model?.op ?? 'AND');
    const [filters, setFilters] = useState<SingleFilter[]>(initialFilters);

    const refInput = useRef<HTMLInputElement>(null);

    useGridFilter({
      doesFilterPass: (_params: IDoesFilterPassParams<any>) => {
        throw new Error("doesFilterPass should not be called in server-side row model mode");
      },
      afterGuiAttached: () => {
        window.setTimeout(() => {
          refInput.current?.focus();
        });
      },
    });

    // Inform grid about filter change
    useEffect(() => {
      onModelChange({ op, filters: filters.filter(f => f.dateFrom) });
    }, [filters, op]);

    // Ensure a blank filter exists at the end once a value is entered
useEffect(() => {
  const last = filters[filters.length - 1];

  // Remove extra trailing blanks beyond the last
  const hasTrailingBlanks =
    filters.length > 1 &&
    filters[filters.length - 1].dateFrom === '' &&
    filters[filters.length - 2].dateFrom === '';

  if (hasTrailingBlanks) {
    setFilters(prev => prev.slice(0, -1)); // remove last blank
    return;
  }

  if (last.dateFrom && !filters.some(f => f.dateFrom === '')) {
    setFilters(prev => [
      ...prev,
      { type: 'greaterThan', dateFrom: '', dateTo: '' },
    ]);
  }
}, [filters]);

    const updateFilter = (index: number, updated: Partial<SingleFilter>) => {
      setFilters(prev => {
        const next = [...prev];
        next[index] = { ...next[index], ...updated };
    
        // Check if current was cleared, and next is empty
        // const current = next[index];
        const after = next[index + 1];
        if (updated.dateFrom === '' && after && after.dateFrom === '') {
          next.splice(index + 1, 1); // remove next
        }
    
        return next;
      });
    };

    return (
      <div className="flex flex-col gap-2 p-2 max-w-[300px] max-h-[60vh] overflow-y-auto">
        <Select
          value={op}
          onValueChange={(val: 'AND' | 'OR') => setOp(val)}
          disabled={filters.filter(f => f.dateFrom).length < 2}
        >
          <SelectTrigger className="w-full">
            <SelectValue placeholder="AND / OR" />
          </SelectTrigger>
          <SelectContent className="ag-custom-component-popup z-[1000]">
            <SelectItem value="AND">AND</SelectItem>
            <SelectItem value="OR">OR</SelectItem>
          </SelectContent>
        </Select>

        {filters.map((filter, i) => (
          <div key={i} className="flex flex-col gap-2 border p-2 rounded">
            <Select
              value={filter.type}
              onValueChange={(val: DateTimeModelType) => updateFilter(i, { type: val })}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select condition" />
              </SelectTrigger>
              <SelectContent className="ag-custom-component-popup z-[1000]">
                {DATE_TIME_OPTIONS.map(opt => (
                  <SelectItem key={opt} value={opt}>
                    {LABELS[opt]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <DateTimePickerPopup
              onChange={(val: string) => updateFilter(i, { dateFrom: val })}
            />

            {filter.type === "inRange" && (
              <DateTimePickerPopup
                onChange={(val: string) => updateFilter(i, { dateTo: val })}
              />
            )}
          </div>
        ))}

        <Button
          onClick={() => {
            onModelChange({ op, filters: filters.filter(f => f.dateFrom) })
            api.hidePopupMenu()
          }}
          className="mt-2 w-full"
        >
          Apply
        </Button>
      </div>
    );
  }
);

export default DateTimeFilterPopup;
