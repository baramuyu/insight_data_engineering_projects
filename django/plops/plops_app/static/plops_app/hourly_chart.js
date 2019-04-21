var chartObj;
function query_for_charts(slot){
    if (chartObj){
        chartObj.destroy();
    }
    var url = "/hourly_api?id=" + slot.station_id;
    $.get(url, function(data, status){
        console.log(data.hourly);
        chartObj = update_charts(data.hourly, slot)
    })
}

function get_label_and_data(hourly){
    labels = [];
    data = [];
    for (var i = 0; i < hourly.length; i++){
        var obj = hourly[i];
        for (var key in obj){
            if (key == 'hour'){
                labels.push(obj[key]);
            }else if(key == 'occupied_spots'){
                data.push(obj[key]);
            }
        }
    }
    return [labels, data]
}

function update_charts(hourly, slot){
    labal_and_data = get_label_and_data(hourly)
    var ctx = document.getElementById('myChart').getContext('2d');
    var myChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labal_and_data[0],
            datasets: [{
                label: 'Overall Average # of occupied spaces',
                data: labal_and_data[1],
                backgroundColor: 'rgba(54, 162, 235, 0.2)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: false,
            scales: {
                xAxes: [ {
                  scaleLabel: {
                    display: true,
                    labelString: 'Hour'
                  },
                  ticks: {
                    major: {
                      fontStyle: 'bold',
                      fontColor: '#FF0000'
                    }
                  }
                } ],
                yAxes: [{
                    ticks: {
                        beginAtZero: true,
                        max: slot.space_count,
                    }
                }]
            },
        }
    });
    return myChart;
}