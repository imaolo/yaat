import React from "react"
import ReactDOM from "react-dom/client"
import App from './App.js'
import 'primereact/./resources/themes/viva-dark/theme.css'

const container = document.getElementById('index')
const root = ReactDOM.createRoot(container)
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)