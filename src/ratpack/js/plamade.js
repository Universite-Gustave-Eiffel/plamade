function loadJobs(mainPanel) {
    mainPanel.innerHTML = "<div class=\"loader\"></div>";
    axios.get('/manage/job_list')
    .then(function (response) {
      // handle success
       let mainPanel = document.getElementById("main");
       if(typeof response.data.redirect !== 'undefined') {
        window.location.href = response.data.redirect;
       } else {
            let tableHtml = "<div class=\"header\"><h1>Manage jobs</h1><h2>Processing job management</h2>" +
            "<table class=\"pure-table pure-table-bordered\">" +
            "    <thead>" +
            "        <tr>" +
            "            <th>job ID</th>" +
            "            <th>Start Date</th>" +
            "            <th>End Date</th>" +
            "            <th>Progression</th>" +
            "            <th>Insee Department</th>" +
            "            <th>Configuration ID</th>" +
            "        </tr>" +
            "    </thead>" +
            "    <tbody>";
            for(let i = 0; i < response.data.length; i++) {
                let m = moment.utc(response.data[i].begin_date);
                tableHtml+= "<tr><td>"+response.data[i].pk_job+"</td><td>"+m.format('LLLL')+"</td><td>"+
                response.data[i].end_date+"</td><td>"+response.data[i].progression+"</td><td>"+
                response.data[i].insee_department+"</td><td>"+response.data[i].conf_id+"</td></tr>";
            }
            tableHtml+= "    </tbody>"+
            "</table></div>";
            console.log(tableHtml);
            mainPanel.innerHTML = tableHtml;
       }
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
};

function subscribe(mainPanel) {
    axios.get('/manage/subscribe')
    .then(function (response) {
       // handle success
       mainPanel.innerHTML = "<div class=\"l-box\"><aside><p>"+response.data.message+"</p></aside></div>";
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
};

function acceptUser(userOoid) {
    axios.get('/manage/user/'+userOoid+'/accept')
    .then(function (response) {
       // handle success
       let mainPanel = document.getElementById("main");
       mainPanel.innerHTML = "";
       loadManageUsers(mainPanel)
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
}


function refuseUser(userOoid) {
    axios.get('/manage/user/'+userOoid+'/refuse')
    .then(function (response) {
       // handle success
       let mainPanel = document.getElementById("main");
       mainPanel.innerHTML = "";
       loadManageUsers(mainPanel)
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
}
function loadManageUsers(mainPanel) {
    mainPanel.innerHTML = "<div class=\"loader\"></div>";
    axios.get('/manage/request_list')
    .then(function (response) {
       // handle success
        let tableHtml = "<div class=\"header\"><h1>Manager users</h1><h2>Accept/Refuse job control requests</h2>" +
        "<table class=\"pure-table pure-table-bordered\">" +
        "    <thead>" +
        "        <tr>" +
        "            <th>ooid</th>" +
        "            <th>mail</th>" +
        "            <th>action</th>" +
        "        </tr>" +
        "    </thead>" +
        "    <tbody>"
        for(let i = 0; i < response.data.length; i++) {
            tableHtml+= "<tr><td>"+response.data[i].ooid+"</td><td>"+response.data[i].email+"</td><td>"+
            "<a class=\"pure-button\" onclick=\"acceptUser('"+response.data[i].ooid+"')\">&CirclePlus;</a> "+
             "<a class=\"pure-button\"  onclick=\"refuseUser('"+response.data[i].ooid+"')\">&CircleMinus;</a></td></tr>"
        }
        tableHtml+= "    </tbody>"+
        "</table></div>"
        mainPanel.innerHTML = tableHtml;
    })
    .catch(function (error) {
      // handle error
      console.log(error);
    })
}

function postAddJob() {
    document.getElementById("addButton").disabled = true;
    let bodyFormData = new FormData();
    bodyFormData.append('INSEE_DEPARTMENT', document.getElementById("INSEE_DEPARTMENT").value);
    bodyFormData.append('CONF_ID', document.getElementById("CONF_ID").value);
    axios({
      method: "post",
      url: "/manage/add_job",
      data: bodyFormData,
      headers: { "Content-Type": "multipart/form-data" },
    })
      .then(function (response) {
        //handle success
        console.log(response);
        document.getElementById("addButton").disabled = false;
      })
      .catch(function (response) {
        //handle error
        console.log(response);
        document.getElementById("addButton").disabled = false;
      });
    return false;
}

function showAddJobForm(mainPanel) {
   mainPanel.innerHTML = "<div class=\"header\"><h1>Add a new job</h1>"+
    "<form class=\"pure-form pure-form-aligned\">"+
    "    <fieldset>"+
    "        <legend>Add a new processing job to the queue</legend>"+
    "        <div class=\"pure-control-group\">"+
    "        <label for=\"INSEE_DEPARTMENT\">Code Department</label>"+
    "        <input type=\"text\" placeholder=\"Code Department\" id=\"INSEE_DEPARTMENT\"/>"+
    "        </div>"+
    "        <div class=\"pure-control-group\">"+
    "        <label for=\"CONF_ID\">Configuration ID</label>"+
    "        <input type=\"text\" placeholder=\"Configuration ID\"  id=\"CONF_ID\"/>"+
    "        </div>"+
    "        <div class=\"pure-controls\">"+
    "        <button id=\"addButton\" type=\"submit\" class=\"pure-button pure-button-primary\" onclick=\"postAddJob()\">Add</button>"+
    "        </div>"+
    "    <fieldset>"+
    "</form>";

}

function locationHashChanged() {
    let mainPanel = document.getElementById("main");
    mainPanel.innerHTML = "";
    switch(location.hash) {
        case "#job_list":
            loadJobs(mainPanel);
            break;
        case "#subscribe":
            subscribe(mainPanel);
            break;
        case "#manage_users":
            loadManageUsers(mainPanel);
            break;
        case "#add_job":
            showAddJobForm(mainPanel);
            break;
        default:
            loadJobs(mainPanel);
            break;
    }
};

window.onhashchange = locationHashChanged;

locationHashChanged();

