// WebSocket URL for the server
const WS_URL = 'ws://127.0.0.1:5000/otb/get_data_ws';
let socket;

// Function to initialize the WebSocket connection
export function initializeWebSocket(onMessageCallback) {
    // Create a WebSocket connection
    console.log("iniitializing ws");
    socket = new WebSocket(WS_URL);

    // Connection opened
    socket.onopen = function (event) {
        console.log('WebSocket connection opened:', event);
    };

    // Listen for messages
    socket.onmessage = function (event) {
        console.log('Received message:', event.data);
        if (typeof onMessageCallback === 'function') {
            onMessageCallback(event.data); // Pass received data to callback
        }
    };

    // Handle WebSocket errors
    socket.onerror = function (error) {
        console.error('WebSocket error:', error);
    };

    // Handle WebSocket close event
    socket.onclose = function () {
        console.log('WebSocket connection closed');
    };

    return socket;
}


// Function to send data via WebSocket
export function sendWebSocketMessage(data) {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify(data));
        console.log('Message sent:', data);
    } else {
        console.error('WebSocket is not open. Cannot send message.');
    }
}

// Function to close WebSocket connection
export function closeWebSocket() {
    if (socket) {
        socket.close();
    }
}

export function getSocket() {
    if (!socket) {
        throw new Error('WebSocket is not initialized. Call initializeWebSocket first.');
    }
    return socket;
}