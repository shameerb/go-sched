<template>
    <div class="task-scheduler">
        <h2>Schedule a task</h2>
        <form @submit.prevent="scheduleTask">
            <div>
                <label for="message">Task Message</label>
                <input v-model="message" id="message" required>
            </div>
            <div>
                <label for="scheduledTime">Scheduled Time:</label>
                <input v-model="scheduledTime" id="scheduledTime" type="datetime-local" required>
            </div>
            <button type="submit">Schedule Task</button>
        </form>
        <p v-if="statusMessage">{{ statusMessage }}</p>
    </div>
</template>

<script>
import axios from 'axios';
import { DateTime } from 'luxon';

export default {
    data() {
        return {
            message: '',
            scheduledTime: '',
            statusMessage:''
        }
    },
    methods: {
        async scheduleTask() {
            try {
                //todo: checkout how to get the host and port from environment
                const date = DateTime.fromISO(this.scheduledTime);
                const localDate = date.toFormat('yyyy-MM-dd\'T\'HH:mm:ssZZ');
                const location = window.location
                const endpoint = `${location.hostname}:${location.port}/api`
                const response = await axios.post(`http://${endpoint}/schedule`, {
                    command: this.message,
                    scheduled_at: localDate
                });
                this.statusMessage = `Task scheduled successfully. ID: ${response.task_id}`;
                this.message = '';
                this.scheduledTime = '';
            } catch (error) {
                this.statusMessage = `Error scheduling task: ${error.message}`;
            }
        }
    }
}
</script>

<style scoped>
    .task-scheduler {
    max-width: 400px;
    margin: 0 auto;
    }
    form > div {
    margin-bottom: 1rem;
    }
    label {
    display: block;
    margin-bottom: 0.5rem;
    }
    input {
    width: 100%;
    padding: 0.5rem;
    }
    button {
    background-color: #4CAF50;
    color: white;
    padding: 0.5rem 1rem;
    border: none;
    cursor: pointer;
    }
</style>