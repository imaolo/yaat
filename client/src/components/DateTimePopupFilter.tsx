import { forwardRef, useRef } from "react";
import type { ChangeEvent } from "react";
import type { IDoesFilterPassParams } from "ag-grid-community";
import type { CustomFilterProps } from "ag-grid-react";
import { useGridFilter } from "ag-grid-react";
import { Button } from "@/components/ui/button";
import InlineDateTimePicker from "@/components/InlineDateTimePicker"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

function getNowAsLocalDateTime(): string {
  const now = new Date();
  now.setSeconds(0, 0);
  const offset = now.getTimezoneOffset();
  const local = new Date(now.getTime() - offset * 60 * 1000);
  return local.toISOString().slice(0, 16);
}

const DATE_TIME_OPTIONS = ['equals', 'notEqual', 'greaterThan', 'lessThan', 'inRange'] as const;
type DateTimeModelType = typeof DATE_TIME_OPTIONS[number];

const LABELS: Record<DateTimeModelType, string> = {
  equals: 'Equals',
  notEqual: 'Not Equal',
  greaterThan: 'After',
  lessThan: 'Before',
  inRange: 'In Range',
};

type DateTimeModel = {
  type: DateTimeModelType;
  dateFrom: string;
  dateTo: string;
};

const DateTimePopupFilter = forwardRef<unknown, CustomFilterProps<any, any, DateTimeModel>>(
  ({ model, onModelChange }, _ref) => {
    const type = model?.type ?? 'greaterThan';
    const dateFrom = model?.dateFrom ?? getNowAsLocalDateTime();
    const dateTo = model?.dateTo ?? "";

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

    // const handleDateFromChange = (e: ChangeEvent<HTMLInputElement>) => {
    //   onModelChange({ type, dateFrom: e.target.value, dateTo });
    // };

    const handleDateToChange = (e: ChangeEvent<HTMLInputElement>) => {
      onModelChange({ type, dateFrom, dateTo: e.target.value });
    };

    const handleTypeChange = (val: DateTimeModelType) => {
      onModelChange({ type: val, dateFrom, dateTo });
    };

    return (
      <div className="flex flex-col gap-2 p-2">
        <Select value={type} onValueChange={handleTypeChange}>
          <SelectTrigger className="w-full">
            <SelectValue placeholder="Select condition" />
          </SelectTrigger>
          <SelectContent className="ag-custom-component-popup z-[1000]">
            {DATE_TIME_OPTIONS.map(opt => (
              <SelectItem key={opt} value={opt}>{LABELS[opt]}</SelectItem>
            ))}
          </SelectContent>
        </Select>

        <InlineDateTimePicker/>

        {type === 'inRange' && (
          <input
            type="datetime-local"
            className="w-full border rounded px-2 py-1"
            value={dateTo}
            onChange={handleDateToChange}
          />
        )}

        <Button
          onClick={() => onModelChange({ type, dateFrom, dateTo })}
          className="mt-2 w-full"
        >
          Apply
        </Button>
      </div>
    );
  }
);

export default DateTimePopupFilter;
