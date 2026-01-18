// Async task handler
addEventListener('task', async (event) => {
    // Simulate async work
    await new Promise(resolve => setTimeout(resolve, 10));

    event.respondWith({
        success: true,
        data: { async: true }
    });
});
