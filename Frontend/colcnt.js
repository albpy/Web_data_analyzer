// import {Bck_fetchDefaultFile} from "./Bck.js";
import {getADANIPORTSDefault} from "./defaultVal.js";
import {dropdownList} from "./dropdown.js";
import {apiStructure} from "./apiStructure.js"
import { initializeWebSocket, getSocket } from "./socket.js";
import { sendfilterdate } from "./submitdate.js";
// import { global_hist_dates } from "./submitdate.js";

document.addEventListener("DOMContentLoaded", function() {
    get_py_data();
});
function get_py_data() 
    {
        // const socket = new WebSocket('ws://127.0.0.1:5000/otb/get_data_ws');
        const dropdown = document.getElementById("dropdownMenu");
        // localStorage.setItem('selectedCompany', dropdown.value);
        dropdown.value = "ADANIPORTS.csv"; 
        // Ensure the connection is established before calling Bck_fetchDefaultFile.
        // The WebSocket connection is created synchronously, but WebSocket connections take some time to establish. 
        // Therefore, it's better to wait until the WebSocket is fully connected before using it in the 
        // Bck_fetchDefaultFile function.
        // socket.addEventListener('open', function (event) {
        //     console.log("WebSocket connection established");

        //     // Now the connection is ready, pass the socket to the function
            
        // });
        getJSONData()
        // selected_data_set = dropdownList()
        // getADANIPORTSDefault().then(data=>{
        //     console.log("Received data:", data);
        //     processCSV(data)
        // }).catch(error=>{
        //     console.error("error fetching data", error);
        // });

        const fileInput = document.getElementById("fileInput");
        
        fileInput.addEventListener("change", function(){
        const file = fileInput.files[0];
        // Read file content
        if (file) {
            const reader = new FileReader();
            reader.onload = function(event) {
                const csvContent = event.target.result;
                console.log("Csv content: ", csvContent);
                displayCSVData(csvContent)
                processCSV(csvContent); // Nove you can process the csv content 
            };
            reader.readAsText(file);
        }else{
            alert("Please select a csv file")
        }
    });
    initializeWebSocket(handleReceivedData)
    const socket = getSocket();
    
    const button = document.getElementById('submitDateRange');
    if (button) {
        button.addEventListener('click', sendfilterdate);
    } else {
        console.error('Submit button not found');
    }
       

// ******************************************Manual Websocket**********************************************
        // socket.onopen = function(event) {
        //     console.log('Connection is open');
        // };
        // socket.onmessage = function(event) {
        //     console.log(`Received message: ${event.data}`);
        //     try {
        //         const parsedData = JSON.parse(event.data);
        //         console.log(parsedData);
        //         displayJSONData(parsedData);
        //     } catch (e) {
        //         console.error('Error parsing JSON:', e);
        //     }
        // };
        // socket.onerror = function(error) {
        //     console.error('Error occured on Websocket connection: ', error)
        // };
        // socket.onclose = function() {
        //     console.log('Websocket connection closed');
        // };
// *********************************************************************************************************

        // Add an event listener to the button
        document.getElementById('cumulativeButton').addEventListener('click', function() {
            sendCumulative(socket);
        });
        // const calenderiframe = document.getElementById('calenderIframe');
        // // ensure iframe is loaded before accessing it's content
        // calenderiframe.onload = function() {
        //     const iframeDocument = calenderiframe.contentDocument || calenderiframe.contentWindow.document;
        //     const submitdate = iframeDocument.getElementById('submitDateRange');
        //     submitdate.addEventListener('click', function() {
        //         const startDate = iframeDocument.getElementById('startDate').value;
        //         const endDate = iframeDocument.getElementById('endDate').value;
        //         console.log('Start Date:', startDate);
        //         console.log('End Date:', endDate);
        //         sendHistdate(socket);
        //     });            
        // };

        // document.getElementById('')
    }

export function handleReceivedData(receivedData) {
    try {
        // Parse the received data (assuming it's JSON)
        const parsedData = JSON.parse(receivedData);
        console.log('Parsed WebSocket data:', parsedData);
        displayJSONData(parsedData);
    } catch (error) {
        console.error('Error parsing WebSocket data:', error);
    }
}

export async function displayJSONData(JSONdata) {
    // Parse JSON data and display with dynamic table
    const data = JSONdata.data;
    const columns = JSONdata.columns;

    console.log('Data:', data); // Log data to verify structure
    console.log('Columns:', columns); // Log columns to verify structure
    // Ensure data and columns are defined
    if (data && columns) {
        const tableHeader = document.getElementById("tableHeader");
        const tableBody = document.getElementById("tableBody");

        //clear existing content
        tableHeader.innerHTML = "";
        tableBody.innerHTML = "";

        // Create table headers from column names
        columns.forEach(column => {
            const th = document.createElement("th");
            th.textContent = column;
            tableHeader.appendChild(th);
        });

        // Create table rows from data
        data.forEach(row => {
            const tr = document.createElement("tr");
            columns.forEach((column, index) => {
                const td = document.createElement("td");
                td.textContent = row[index] || ""; // Access row data using index
                tr.appendChild(td);
            });
            tableBody.appendChild(tr);
        });
    } else {
        console.error('Data or columns are undefined.');
    }
}


async function getJSONData() {
    console.log('we sre in getJSONData')
    const dropdown = document.getElementById("dropdownMenu");
    const SelectedValue = dropdown.value;
    console.log(SelectedValue)
    // let JSONdata = null;
    var JSONdata = await getADANIPORTSDefault();
    dropdown.addEventListener('change', async function () {
        const SelectedValue = dropdown.value;
            console.log(SelectedValue)
            JSONdata = await dropdownList(SelectedValue);
            displayJSONData(JSONdata);

        // }
    })
    // console.log('Data of display:', JSONdata); // This is a JavaScript object
    // Check if jsonData is valid
    if (!JSONdata) {
        console.error('Failed to retrieve or parse data.');
        return;
    }
    displayJSONData(JSONdata);
}

function sendCumulative(socket) {
    console.log("sendcumulative")
    if (socket && socket.readyState === WebSocket.OPEN) {
        apiStructure.cumulative = true; // Update cumulative field
        apiStructure.company = document.getElementById('dropdownMenu').value;
        // Send the message over the WebSocket
        socket.send(JSON.stringify(apiStructure));
        console.log("Cumulative sent:", apiStructure);
        apiStructure.cumulative = false;
    } else {
        console.log("WebSocket is not connected.");
    }
}

// addEventListener() method attaches an event handler to an element without overwriting existing event handlers. 
// You can add many event handlers to one element. You can add many event handlers of the same type to one element, 
// i.e two "click" events.
