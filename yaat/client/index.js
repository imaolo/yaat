import React from "react"
import ReactDOM from "react-dom/client"
import App from './App.js'

const container = document.getElementById('index')
const root = ReactDOM.createRoot(container)
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)