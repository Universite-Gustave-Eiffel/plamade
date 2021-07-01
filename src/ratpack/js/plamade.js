
function loadCompletedJobs(mainPanel) {
    let mainDiv = document.createElement("div");
    mainDiv.className = "loader";
    mainPanel.appendChild(mainDiv);
}

function loadJobsInProgress(mainPanel) {

}


function loadAddJob(mainPanel) {

}

function locationHashChanged() {
    let mainPanel = document.getElementById("main");
    mainPanel.innerHTML = "";
    switch(location.hash) {
        case "#jobs_completed":
            loadCompletedJobs(mainPanel);
            break;
        case "#jobs_in_progress":
            loadJobsInProgress(mainPanel);
            break;
        case "#add_job":
            loadAddJob(mainPanel);
            break;
    }
}

function generateTable(table, data) {

}


window.onhashchange = locationHashChanged;