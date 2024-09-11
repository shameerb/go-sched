htmx.on("htmx:afterRequest", function(evt) {
    var statusMessage = document.getElementById("status-message");
    if (evt.detail.successful) {
        statusMessage.classList.remove("status-error");
        statusMessage.textContent = "Task scheduled successfully (simulated)";
    } else {
        statusMessage.classList.add("status-error");
        statusMessage.textContent = "Error scheduling task (simulated)";
    }
});

// Simulate the server response
// htmx.defineExtension('simulate-response', {
//     onEvent: function (name, evt) {
//         if (name === "htmx:beforeRequest") {
//             evt.detail.xhr = {
//                 getAllResponseHeaders: function () { return ""; },
//                 getResponseHeader: function () { return ""; },
//                 response: "",
//                 responseText: "",
//                 status: 200,
//             };
//             evt.detail.successful = true;
//             evt.stopPropagation();
//         }
//     }
// });