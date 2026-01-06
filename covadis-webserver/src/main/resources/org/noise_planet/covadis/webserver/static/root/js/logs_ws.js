// small helper function for selecting element by id
let id = id => document.getElementById(id);

function openJobLogsStream(url) {
    console.log("Opening WebSocket connection to " + url);
    //Establish the WebSocket connection and set up event handlers
    let ws = new WebSocket(url);
    ws.onmessage = msg => addLogline(msg)
    ws.onerror = () => console.error("WebSocket error: " + ws.readyState);
}

function addLogline(msg) { // Add log line to html
    id("logs").insertAdjacentText("afterbegin", msg.data);
}
