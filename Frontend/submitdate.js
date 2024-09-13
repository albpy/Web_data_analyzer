// Function to handle date filtering
// import {apiStructure} from "./apiStructure.js"
import { initializeWebSocket,getSocket } from "./socket.js";
import { handleReceivedData, displayJSONData} from "./colcnt.js";
import { apiStructure } from "./apiStructure.js";

export const global_hist_dates = {
    startDate : null, 
    endDate : null
}

export function sendfilterdate() {
    const startdate = document.getElementById('startDate').value;
    const enddate = document.getElementById('endDate').value;
    
    if (startdate === "" || enddate === "") {
        alert('Please select both start and end dates.');
    } else {
        console.log('Start Date:', startdate); // Log start date value
        console.log('End Date:', enddate);     // Log end date value
        global_hist_dates.startDate = startdate;
        global_hist_dates.endDate = enddate;
        // initializeWebSocket(handleReceivedData)
        // // Use a timeout to ensure WebSocket is open
        // setTimeout(() => {
            const socket = getSocket();
        //     console.log("Sending hist filtering dates");
        //     sendHistdate(socket);
        // }, 1000); // Adjust the delay as needed
        sendHistdate(socket);
    }
}

// // Ensure DOM is fully loaded before adding event listener
// document.addEventListener('DOMContentLoaded', function() {
//     console.log('DOM fully loaded and parsed'); // Log when DOM is ready
//     const button = document.getElementById('submitDateRange');
//     if (button) {
//         button.addEventListener('click', sendfilterdate);
//     } else {
//         console.error('Submit button not found');
//     }
// });
// // const tableHeader = document.getElementById("tableHeader");
// const tableBody = document.getElementById("tableBody");

function sendHistdate(socket, s) {
    // initializeWebSocket(handleReceivedData)

    if (socket && socket.readyState === WebSocket.OPEN) {
        apiStructure.history_date_range.fro = global_hist_dates.startDate;
        apiStructure.history_date_range.to = global_hist_dates.endDate;
        apiStructure.company = localStorage.getItem('selectedCompany');
        socket.send(JSON.stringify(apiStructure));
        console.log("apistructure", apiStructure)
        console.log("sent hist filtering dates")
    }else {
        console.log("WebSocket is not connected hist dates.");
    }
}
