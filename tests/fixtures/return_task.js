// Task using return instead of respondWith
addEventListener('task', (event) => {
    return {
        success: true,
        data: {
            method: 'return',
            payload: event.payload
        }
    };
});
