htmx.on("htmx:wsOpen", function(event) {
    console.log("WebSocket connection established");
});

htmx.on("htmx:wsClose", function(event) {
    console.log("WebSocket connection closed");
    // Attempting to reconnect every 5 seconds
    setTimeout(function() {
        htmx.trigger("#execution-list", "reconnect");
    }, 5000);
});

htmx.on("htmx:wsError", function(event) {
    console.error("WebSocket error: ", event.detail.error);
});

htmx.on("htmx:wsMessage", function(event) {
    var execution = JSON.parse(event.detail.message);
    console.log("execution: " + execution);
    var newItem = document.createElement("li");
    
    newItem.className = "execution-item";
    newItem.innerHTML = `
        <div class="execution-detail">
            <span class="label">Task ID: </span>
            <span class="value">${execution.id}</span>
        </div>
        <div class="execution-detail">
            <span class="label">Message: </span>
            <span class="value">${execution.command}</span>
        </div>
    `;
    document.getElementById("execution-list").appendChild(newItem);
});