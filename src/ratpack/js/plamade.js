function generateTable(table, data) {

};

function loadCompletedJobs(mainPanel) {
    let mainDiv = document.createElement("div");
    mainDiv.className = "loader";
    mainPanel.appendChild(mainDiv);
    axios.get('/manage/joblist')
    .then(function (response) {
      // handle success
       let mainPanel = document.getElementById("main");
       if(typeof response.data.redirect !== 'undefined') {
        window.location.href = response.data.redirect;
       }
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

function subscribe(mainPanel) {
    cleanPanel(mainPanel);
    axios.get('/manage/subscribe')
    .then(function (response) {
       // handle success
       mainPanel.innerHTML = "<div class="l-box"><aside><p>"+response.data.message+"</p></aside></div>";
    })
    .catch(function (error) {
      // handle error
      console.log(error);
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
        case "#subscribe":
            subscribe(mainPanel);
            break;
        default:
            loadAddJob(mainPanel);
            break;
    }
};

window.onhashchange = locationHashChanged;

locationHashChanged();

