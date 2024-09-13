export async function getADANIPORTSDefault() {
    try {
        // Perform the GET request
        const response = await fetch('http://127.0.0.1:5000/otb/get_default');
        
        // Check if the response is ok
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        
        // Parse the JSON response
        const result = await response.json();
        
        // Access the JSON string from the result
        const dataJson = result.data;
        
        // Parse the JSON string into a JavaScript object
        const dataObject = JSON.parse(dataJson);
        
        console.log('Parsed Data:', dataObject); // This is a JavaScript object
        
        return dataObject
        // Use the data as needed
        // e.g., populate a table or chart
        
    } catch (error) {
        console.error('There was a problem with the fetch operation:', error);
    }
}

// export async function populateDropdown() {
//     // Perform the GET request
//     const response = await fetch('http://127.0.0.1:5000/otb/get_available_companies');
//     // Check if the response is ok
//     if (!response.ok) {
//         throw new Error('Network response was not ok');
//     }

//     // Parse the JSON response
//     const result = await response.json();
        
//     // Access the JSON string from the result
//     const dataJson = result.data;
    
//     // Parse the JSON string into a JavaScript object
//     const dataObject = JSON.parse(dataJson);
    
//     console.log('Parsed Data:', dataObject); // This is a JavaScript object 
// }