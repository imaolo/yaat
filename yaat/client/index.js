
import React from "react";
import ReactDOM from "react-dom/client";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";
import { DataGrid } from "@mui/x-data-grid";

function DataTable({ data, schema }) {
  const columns = Object.keys(schema.properties).map((key) => ({
    field: key,
    headerName: key.charAt(0).toUpperCase() + key.slice(1),
    flex: 1,
  }));

  const new_data = data.map(({ _id, ...rest }) => rest);
  const rows = new_data.map((item, index) => ({ id: index, ...item }));

  return (
    <div style={{ height: 400, width: "100%" }}>
      <DataGrid rows={rows} columns={columns} pageSize={5} />
    </div>
  );
}

function Index() {
  const [schema, setSchema] = React.useState(null);
  const [queries, setQueries] = React.useState(null);

  const handleSubmit = ({ formData }) => {
    fetch("/TopMoverDoc", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(formData),
    })
    .then((response) => {
      if (!response.ok)
        throw new Error("Network response was not ok");
      return response.json();
    })
    .then((newDoc) => {
      setQueries([...queries, newDoc]);
    })
    .catch((error) => {
      console.error("Error submitting form:", error);
    });
  };

  React.useEffect(() => {
    fetch("/schema/TopMoverDoc")
      .then(response => response.json())
      .then(data => {
        setSchema(data)
      })
    fetch("/TopMoverDoc")
      .then(response => response.json())
      .then(data => {
        setQueries(data)});
  }, [])

  if (!schema || !queries)
    return <div>Loading...</div>;

  return (
    <div>
      <h1>Dynamic Form from JSON Schema new message</h1>
      <Form schema={schema} validator={validator} onSubmit={handleSubmit}/>
      <h2>Existing Queries</h2>
      <DataTable data={queries} schema={schema} />
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('index')).render(<Index />);