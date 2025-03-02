
import React from "react";
import ReactDOM from "react-dom/client";
import Form from "@rjsf/core";
import validator from "@rjsf/validator-ajv8";

function Index() {
  const [schema, setSchema] = React.useState(null);

    React.useEffect(() => {
      fetch("/api/schema")
        .then(response => response.json())
        .then(data => setSchema(data))
      }, [])

    if (!schema)
      return <div>Loading...</div>;
  
    return (
      <div>
        <h1>Dynamic Form from JSON Schema</h1>
        <Form schema={schema} validator={validator} />
      </div>
    );

}

ReactDOM.createRoot(document.getElementById('index')).render(<Index />);