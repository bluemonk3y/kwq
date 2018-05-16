var rawResponseBody = '';
var xhr = new XMLHttpRequest();

String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.split(search).join(replacement);
};



function displayServerVersion() {
    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            var serverVersionResponse = JSON.parse(this.responseText);
            document.getElementById("copyright").innerHTML = "(c) Confluent Inc., KSQL server v" + serverVersionResponse.KsqlServerInfo.version
        }
    };
    xhr.open("GET", "/info", true);
    xhr.send();
}

function sendRequest(resource, sqlExpression) {
    xhr.abort();

    var properties = getProperties();

    xhr.onreadystatechange = function() {
        if (xhr.response !== '' && ((xhr.readyState === 3 && streamedResponse) || xhr.readyState === 4)) {
            rawResponseBody = xhr.response;
            renderResponse();
        }
        if (xhr.readyState === 4 || xhr.readyState === 0) {
            document.getElementById('request_loading').hidden = true;
            document.getElementById('cancel_request').hidden = true;
        }
    };

    var data = JSON.stringify({
        'ksql': sqlExpression,
        'streamsProperties': properties
    });

    console.log("Sending:" + data)

    document.getElementById('cancel_request').hidden = false;
    document.getElementById('request_loading').hidden = false;
    xhr.open('POST', resource);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.send(data);
}

function cancelRequest() {
    //var responseElement = document.getElementById('response');
    //var response = responseElement.innerHTML;
    xhr.abort();
    //responseElement.innerHTML = response;
}

function renderResponse() {
    var renderedBody = '';
    var parsedJson = JSON.parse(rawResponseBody);
//    if (streamedResponse) {
//        // Used to try to report JSON parsing errors to the user, but
//        // since printed topics don't have a consistent format, just
//        // have to assume that any parsing error is for that reason and
//        // we can just stick with the raw message body for the output
//        var splitBody = rawResponseBody.split('\n');
//        for (var i = 0; i < splitBody.length; i++) {
//            var line = splitBody[i].trim();
//            if (line !== '') {
//                try {
//                    var parsedJson = JSON.parse(line);
//                    renderedBody += renderFunction(parsedJson);
//                } catch (SyntaxError) {
//                    renderedBody += line;
//                }
//                renderedBody += '\n';
//            }
//        }
//    } else {
//        try {
//            var parsedJson = JSON.parse(rawResponseBody);
//            renderedBody = renderFunction(parsedJson);
//
//            if (renderedBody == "") {
//                updateFormat(renderPrettyJson)
//                renderFunction = renderTabular;
//                return;
//            }
//
//
//        } catch (SyntaxError) {
//            console.log('Error parsing response JSON:' + SyntaxError.message);
//            console.log(SyntaxError.stack);
//            renderedBody = rawResponseBody;
//        }
//    }
//    response.setValue(renderedBody);
//    response.gotoLine(1);
}


function upperCaseFirst(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
}
function isPrimitive(test) {
    return (test !== Object(test));
};

function createTable() {
    var dataSet = []
    taskTable = $('#taskTable').DataTable( {
        data: dataSet,
        "columns" : [ { title: "Task-Id",
                            "data" : "id"
                        }, {title: "Group",
                            "data" : "groupId"
                        }, {
                            title: "Priority",
                            "data" : "priority"
                        }, {
                            title: "Tag",
                            "data" : "tag"
                        }, {
                            title: "Source",
                           "data" : "source"
                        }, {
                            title: "Status",
                          "data" : "status"
                        }, {
                            title: "Submitted",
                          "data" : "submitted"
                        }, {
                            title: "Allocated",
                          "data" : "allocated"
                        }, {
                            title: "Running",
                          "data" : "running"
                        }, {
                            title: "Completed",
                          "data" : "completed"
                        }, {
                            title: "Duration",
                          "data" : "duration"
                        }

                         ]
    } );
    $('#refreshTable').click(refreshTable)
}

function refreshTable() {
    $.get({
          url:"/kwq/tasks",
          success:function(data){
                taskTable.clear();
                data.forEach(e => {
                    e.duration = ""
                    if (e.submitted_ts != 0) { e.submitted = moment(e.submitted_ts).format("HH:mm:ss") }
                    else { e.submitted = "" }

                    if (e.allocated_ts != 0) { e.allocated = moment(e.allocated_ts).format("HH:mm:ss") }
                    else { e.allocated = "" }

                    if (e.running_ts != 0) { e.running = moment(e.running_ts).format("HH:mm:ss") }
                    else { e.running = ""}

                    if (e.completed_ts != 0) {
                        e.completed = moment(e.completed_ts).format("HH:mm:ss")
                        e.duration = (e.completed_ts - e.running_ts) / 1000
                        }
                    else { e.completed = "" }
                })
                taskTable.rows.add(data);
                taskTable.draw();
            return false;
          }
    })
}

function randomNumber(min, max) {
			return Math.random() * (max - min) + min;
}

function randomBar(date, lastClose) {
    var open = randomNumber(lastClose * 0.95, lastClose * 1.05);
    var close = randomNumber(open * 0.95, open * 1.05);
    return {
        t: date.valueOf(),
        y: 0//close
    };
}

function createChart() {
    console.log("Loading mainChart")
    var date = moment().subtract(1, 'hour');

    var data = [randomBar(date, 30)];
    while (data.length < 60) {
        date = date.clone().add(1, 'minute');
        data.push(randomBar(date, data[data.length - 1].y));
    }

    // TODO add labels dynamically https://github.com/chartjs/Chart.js/issues/2738
    var ctx = document.getElementById('mainChart').getContext('2d');
    var cfg = {
        type: 'bar',
        data: {
            datasets: [{
                label: 'Task status',
                data: data,
                type: 'line',
                pointRadius: 1,
                fill: false,
                lineTension: 0,
                borderWidth: 2
            }]
        },
        options: {
            scales: {
                xAxes: [{
                    type: 'time',
                    distribution: 'series'
                }],
                yAxes: [{
                    scaleLabel: {
                        display: true,
                        labelString: 'Task status by Job'
                    }
                }]
            }
        }
    };
chart = new Chart(ctx, cfg);
}

function removeChartData(chart) {
    chart.data.labels.pop();
    chart.data.datasets.forEach((dataset) => {
        dataset.data.pop();
    });
    chart.update();
}

function addChartData(label, data) {
    //chart.data.labels.push(label);
    chart.data.datasets.forEach((dataset) => {
        dataset.data.push(data);
    });
    chart.update();
}


/**
{
  "total": 0,
  "running": 0,
  "error": 0,
  "completed": 0
}
**/
function refreshChart() {
    $.get({
          url:"/kwq/stats",
          success:function(data){
                data.forEach(e => {
                    chart.data.datasets.forEach((dataset) => {
                        dataset.data.push(e);
                    });
                })
                chart.update();
            return false;
          }
    })
}

function createStuff() {
    createChart();
    createTable();
    refreshTable();

}
window.onload = createStuff;

