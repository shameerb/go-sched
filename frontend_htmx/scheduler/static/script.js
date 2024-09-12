htmx.on("htmx:afterRequest", function(evt) {
    
});

function adjustDateTime(event) {
    const form = event.target;
    const scheduledAtInput = form.querySelector('#scheduled_at');
    const scheduledAt = new Date(scheduledAtInput.value);

    // Format the date as "YYYY-MM-DDTHH:mm:ss+HH:mm"
    const formattedDate = scheduledAt.toISOString().slice(0, 19) + scheduledAt.toString().match(/([+-]\d{2}:\d{2}|Z)$/)[0];

    // Create a new hidden input with the formatted date
    const hiddenInput = document.createElement('input');
    hiddenInput.type = 'hidden';
    hiddenInput.name = 'scheduled_at';
    hiddenInput.value = formattedDate;

    // Replace the original input in the form data
    form.appendChild(hiddenInput);
    scheduledAtInput.disabled = true;
}

function showStatusMessage(event) {
    const statusMessage = document.getElementById('status-message');
    console.log("statusMessage : " + statusMessage);
    if (event.detail.successful) {
        statusMessage.classList.remove("status-error");
        statusMessage.textContent = "Task scheduled successfully";
    } else {
        statusMessage.classList.add("status-error");
        statusMessage.textContent = "Error scheduling task";
    }
    statusMessage.style.display = 'block';
}

// Function to hide status message
function hideStatusMessage(event) {
    const statusMessage = document.getElementById('status-message');
    statusMessage.style.display = 'none';
}


document.addEventListener('DOMContentLoaded', function() {
    const form = document.querySelector('form');
    
    // Hide status message initially
    hideStatusMessage();

    // Add event listeners for htmx events
    form.addEventListener('htmx:beforeRequest', adjustDateTime);
    form.addEventListener('htmx:afterRequest', showStatusMessage);
    form.addEventListener('htmx:beforeRequest', hideStatusMessage);
});
