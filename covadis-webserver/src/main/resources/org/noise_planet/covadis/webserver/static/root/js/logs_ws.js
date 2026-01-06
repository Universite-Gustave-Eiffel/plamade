// small helper function for selecting element by id
let id = id => document.getElementById(id);

function openJobLogsStream(url) {
    console.log("Opening WebSocket connection to " + url);
    //Establish the WebSocket connection and set up event handlers
    let ws = new WebSocket(url);
    ws.onmessage = msg => addLogline(msg);
    ws.onclose = () => alert("WebSocket connection closed");
}

function addLogline(msg) { // Add log line to html
    id("logs").insertAdjacentText("afterbegin", msg.data);
}
