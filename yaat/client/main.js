function Main() {
  const [tabs, setTabs] = React.useState([]);

  React.useEffect(() => {
    fetch("/api/tabs")
      .then(response => response.json())
      .then(data => setTabs(data.tabs))
  }, []);

  return (
      <div>
          <h1>Minimal React App</h1>
          <div>
              {tabs.map(tab => (<button key={tab}>{tab}</button>))}
          </div>
      </div>
  );
}

ReactDOM.createRoot(document.getElementById('main')).render(<Main />);