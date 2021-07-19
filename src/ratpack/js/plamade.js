
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
