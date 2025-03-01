document.addEventListener("DOMContentLoaded", function() {
    // Select the element with id="header"
    const header = document.getElementById("header");
    
    // Change its text content to confirm the JS is running
    header.textContent = "JavaScript is working!";
    
    // Optionally, add a new element to the page
    const newParagraph = document.createElement("p");
    newParagraph.textContent = "This paragraph was added by JavaScript.";
    document.body.appendChild(newParagraph);
  });