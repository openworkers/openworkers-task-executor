// Basic task handler
addEventListener('task', (event) => {
    event.respondWith({
        success: true,
        data: {
            taskId: event.taskId,
            received: event.payload
        }
    });
});
