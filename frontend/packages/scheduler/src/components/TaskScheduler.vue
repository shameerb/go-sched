<template>
  <div class="task-scheduler">
    <h2 class="title">Schedule a Task</h2>
    <form @submit.prevent="scheduleTask" class="form">
      <div class="form-group">
        <label for="message" class="label">Task Message</label>
        <input v-model="message" id="message" required class="input">
      </div>
      <div class="form-group">
        <label for="scheduledTime" class="label">Scheduled Time</label>
        <input v-model="scheduledTime" id="scheduledTime" type="datetime-local" required class="input">
      </div>
      <button type="submit" class="button">Schedule Task</button>
    </form>
    <p v-if="statusMessage" class="status-message" :class="{ 'status-error': statusMessage.includes('Error') }">
      {{ statusMessage }}
    </p>
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
      statusMessage: ''
    }
  },
  methods: {
    async scheduleTask() {
      try {
        const date = DateTime.fromISO(this.scheduledTime);
        const localDate = date.toFormat('yyyy-MM-dd\'T\'HH:mm:ssZZ');
        const location = window.location
        const endpoint = `${location.hostname}:${location.port}/api`
        const response = await axios.post(`http://${endpoint}/schedule`, {
          command: this.message,
          scheduled_at: localDate
        });
        this.statusMessage = `Task scheduled successfully. ID: ${response.data.task_id}`;
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
  margin: 2rem auto;
  padding: 2rem;
  background-color: #f8f9fa;
  border-radius: 8px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}

.title {
  color: #343a40;
  font-size: 1.5rem;
  margin-bottom: 1.5rem;
  text-align: center;
}

.form {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.form-group {
  display: flex;
  flex-direction: column;
}

.label {
  font-weight: 600;
  margin-bottom: 0.5rem;
  color: #495057;
}

.input {
  width: 100%;
  padding: 0.75rem;
  border: 1px solid #ced4da;
  border-radius: 4px;
  font-size: 1rem;
  transition: border-color 0.15s ease-in-out;
}

.input:focus {
  outline: none;
  border-color: #4dabf7;
  box-shadow: 0 0 0 2px rgba(77, 171, 247, 0.25);
}

.button {
  background-color: #4dabf7;
  color: white;
  padding: 0.75rem 1rem;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1rem;
  font-weight: 600;
  transition: background-color 0.15s ease-in-out;
}

.button:hover {
  background-color: #3793dd;
}

.status-message {
  margin-top: 1rem;
  padding: 0.75rem;
  border-radius: 4px;
  background-color: #d4edda;
  color: #155724;
  text-align: center;
}

.status-error {
  background-color: #f8d7da;
  color: #721c24;
}
</style>