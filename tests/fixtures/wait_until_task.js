// Task with waitUntil
globalThis.backgroundDone = false;

addEventListener('task', (event) => {
    // Respond immediately
    event.respondWith({
        success: true,
        data: { immediate: true }
    });

    // Background work via waitUntil
    event.waitUntil(new Promise(resolve => {
        setTimeout(() => {
            globalThis.backgroundDone = true;
            resolve();
        }, 20);
    }));
});
