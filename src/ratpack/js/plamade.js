function generateTable(table, data) {

};

function loadCompletedJobs(mainPanel) {
    let mainDiv = document.createElement("div");
    mainDiv.className = "loader";
    mainPanel.appendChild(mainDiv);
    axios.get('manage/joblist')
    .then(function (response) {
      // handle success
       let mainPanel = document.getElementById("main");
       generateTable(mainPanel, response);
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
    .then(function () {
      // always executed
      cleanPanel(mainPanel);
    })
};

function loadJobsInProgress(mainPanel) {

};


function loadAddJob(mainPanel) {

};

function cleanPanel(mainPanel) {
    mainPanel.innerHTML = "";

}

function locationHashChanged() {
    let mainPanel = document.getElementById("main");
    cleanPanel(mainPanel);
    switch(location.hash) {
        case "#jobs_completed":
            loadCompletedJobs(mainPanel);
            break;
        case "#jobs_in_progress":
            loadJobsInProgress(mainPanel);
            break;
        default:
            loadAddJob(mainPanel);
            break;
    }
};

window.onhashchange = locationHashChanged;

locationHashChanged();

