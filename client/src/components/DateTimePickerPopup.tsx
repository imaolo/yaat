import { useState, useRef, useEffect } from "react"
import { Calendar } from "@/components/ui/calendar"
import { Input } from "@/components/ui/input"
import { format } from "date-fns"
import { cn } from "@/lib/utils"

export default function DateTimePickerPopup({ onChange }: any) {
  const [dateTime, setDateTime] = useState<Date | null>(null)
  const [inputValue, setInputValue] = useState("")
  const [showPicker, setShowPicker] = useState(false)
  const [popupPosition, setPopupPosition] = useState<{ top: number; left: number }>({ top: 0, left: 0 })

  const inputRef = useRef<HTMLInputElement>(null)
  const popupRef = useRef<HTMLDivElement>(null)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value
    setInputValue(val)
    onChange(val)
    const parsed = new Date(val)
    if (!isNaN(parsed.getTime())) setDateTime(parsed)
  }

  const handleCalendarSelect = (date: Date | undefined) => {
    if (!date) return
    const time = dateTime ?? new Date()
    const newDate = new Date(date)
    newDate.setHours(time.getHours(), time.getMinutes())
    let val = format(newDate, "yyyy-MM-dd'T'HH:mm")
    setInputValue(val)
    onChange(val)
  }

  const handleTimeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const [h, m] = e.target.value.split(":").map(Number)
    if (isNaN(h) || isNaN(m)) return
    const newDate = dateTime ? new Date(dateTime) : new Date()
    newDate.setHours(h, m)
    setDateTime(newDate)
    let val = format(newDate, "yyyy-MM-dd'T'HH:mm")
    setInputValue(val)
    onChange(val)
  }

  const showPopup = () => {
    const rect = inputRef.current?.getBoundingClientRect()
    if (rect) {
      setPopupPosition({ top: rect.bottom + window.scrollY + 4, left: rect.left + window.scrollX })
      setShowPicker(true)
    }
  }

// useEffect(() => {
//   const handleClickOutside = (event: MouseEvent) => {
//     const target = event.target as Node;

//     const clickedInside =
//       inputRef.current?.contains(target) ||
//       popupRef.current?.contains(target);

//     if (!clickedInside) {
//       setShowPicker(false);
//     }
//   };

//   const interceptClickEarly = (e: MouseEvent) => {
//     const target = e.target as Node;

//     const insidePopup =
//       popupRef.current?.contains(target) ||
//       inputRef.current?.contains(target);

//     if (insidePopup) {
//       e.stopImmediatePropagation(); // this is the key to beat AG Grid
//     }
//   };

//   if (showPicker) {
//     document.addEventListener("mousedown", interceptClickEarly, true); // capture phase
//     document.addEventListener("mousedown", handleClickOutside);
//   }

//   return () => {
//     document.removeEventListener("mousedown", interceptClickEarly, true);
//     document.removeEventListener("mousedown", handleClickOutside);
//   };
// }, [showPicker]);


useEffect(() => {
  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as Node
    if (
      !inputRef.current?.contains(target) &&
      !popupRef.current?.contains(target)
    ) {
      setShowPicker(false)
    }
  }

  const handleKeyDown = (event: KeyboardEvent) => {
    if (event.key === "Enter") {
      setShowPicker(false)
    }
  }

  if (showPicker) {
    document.addEventListener("mousedown", handleClickOutside)
    document.addEventListener("keydown", handleKeyDown)
  }

  return () => {
    document.removeEventListener("mousedown", handleClickOutside)
    document.removeEventListener("keydown", handleKeyDown)
  }
}, [showPicker])


  return (
    <>
      <Input
        ref={inputRef}
        value={inputValue}
        placeholder="YYYY-MM-DDTHH:MM"
        onChange={handleInputChange}
        onFocus={showPopup}
      />


      
      {showPicker && (
        <div
          ref={popupRef}
          style={{ position: "fixed", top: popupPosition.top, left: popupPosition.left }}
          className={cn(
            "max-w-[300px] w-[280px] border rounded-md shadow-md bg-white p-3",
            "ag-custom-component-popup ag-popup z-[9999]"
          )}
        >
          <Calendar
            mode="single"
            selected={dateTime ?? new Date()}
            onSelect={handleCalendarSelect}
          />
          <div className="mt-2 flex items-center gap-2">
            <label className="text-sm">Time:</label>
            <input
              type="time"
              value={dateTime ? format(dateTime, "HH:mm") : ""}
              onChange={handleTimeChange}
              className="border rounded px-2 py-1 text-sm w-full"
            />
          </div>
          <div className="text-right mt-2">
            <button
              className="text-sm text-blue-600 hover:underline"
              onClick={() => setShowPicker(false)}
            >
              Close
            </button>
          </div>
        </div>
      )}
    </>
  )
}
