<template>
    <div class="task-execution-viewer">
        <h2>Task Execution</h2>
        <ul>
            <li v-for="execution in executions" :key="execution.id">
                <strong>TaskId:</strong>{{ execution.id }}<br>
                <strong>Message:</strong>{{ execution.command }}<br>
                <!-- <strong>Executed at:</strong> {{ new Date(execution.executedAt).toLocaleString() }} -->
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
            // todo: this needs to be picked up during build
            // const endpoint = import.meta.env.VITE_WEB_API || "localhost:8100"
            const location = window.location
            
            const endpoint = `${location.hostname}:${location.port}`
            this.socket = new WebSocket(`ws://${endpoint}/ws`);
            this.socket.onopen = () => {
                console.log('websocket connection established');
            };
            // this.socket.connect = () => {};
            this.socket.onmessage = (event) => {
                const newExecution = JSON.parse(event.data);
                this.executions.push(newExecution);
            };
            this.socket.onerror = (error) => {
                console.error('websocket error: ', error);
            };
            this.socket.onclose = () => {
                console.log("websocket connection closed");
                // attempting to reconnect every 5 sec
                setTimeout(this.connectWebSocket, 5000);
            }
        }
    }
}
</script>

<style scoped>
    .task-execution-viewer {
    max-width: 600px;
    margin: 0 auto;
    }
    ul {
    list-style-type: none;
    padding: 0;
    }
    li {
    border: 1px solid #ddd;
    margin-bottom: 1rem;
    padding: 1rem;
    }
</style>