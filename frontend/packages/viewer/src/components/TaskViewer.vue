<template>
  <div class="task-execution-viewer">
    <h2 class="title">Task Execution</h2>
    <ul class="execution-list">
      <li v-for="execution in executions" :key="execution.id" class="execution-item">
        <div class="execution-detail">
          <span class="label">Task ID: </span>
          <span class="value">{{ execution.id }}</span>
        </div>
        <div class="execution-detail">
          <span class="label">Message: </span>
          <span class="value">{{ execution.command }}</span>
        </div>
      </li>
    </ul>
  </div>
</template>

<script>
export default {
  data() {
    return {
      executions: [],
      socket: null
    }
  },
  mounted() {
    this.connectWebSocket();
  },
  methods: {
    connectWebSocket() {
      const location = window.location
      const endpoint = `${location.hostname}:${location.port}`
      this.socket = new WebSocket(`ws://${endpoint}/ws`);
      
      this.socket.onopen = () => {
        console.log('WebSocket connection established');
      };
      
      this.socket.onmessage = (event) => {
        const newExecution = JSON.parse(event.data);
        this.executions.push(newExecution);
      };
      
      this.socket.onerror = (error) => {
        console.error('WebSocket error: ', error);
      };
      
      this.socket.onclose = () => {
        console.log("WebSocket connection closed");
        // Attempting to reconnect every 5 seconds
        setTimeout(this.connectWebSocket, 5000);
      }
    }
  }
}
</script>

<style scoped>
.task-execution-viewer {
  max-width: 400px; /* Reduced from 600px */
  margin: 1rem auto; /* Reduced top and bottom margin */
  padding: 1rem; /* Reduced padding */
  background-color: #f8f9fa;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Reduced shadow */
}

.title {
  color: #343a40;
  font-size: 1.25rem; /* Slightly reduced font size */
  margin-bottom: 1rem; /* Reduced margin */
  text-align: center;
}

.execution-list {
  list-style-type: none;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 0.75rem; /* Reduced gap */
}

.execution-item {
  background-color: #ffffff;
  border: 1px solid #e9ecef;
  border-radius: 4px;
  padding: 0.75rem; /* Reduced padding */
  transition: box-shadow 0.3s ease;
}

.execution-item:hover {
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1); /* Reduced shadow on hover */
}

.execution-detail {
  display: flex;
  margin-bottom: 0.25rem; /* Reduced margin */
  font-size: 0.9rem; /* Slightly reduced font size */
}

.execution-detail:last-child {
  margin-bottom: 0;
}

.label {
  font-weight: 600;
  color: #495057;
  width: 70px; /* Reduced width */
  flex-shrink: 0;
}

.value {
  color: #212529;
  word-break: break-word;
}
</style>