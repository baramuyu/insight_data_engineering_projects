"use strict";

var parking_markers = [];

function findParking(latLng){
    console.log('finding parking.', latLng);
    var lat = latLng.lat().toString()
    var lng = latLng.lng().toString()
    var url = `/app/api?lat=${lat}&lng=${lng}`;
    console.log("url:" + url)
    $.get(url, function(data, status){
        console.log(data.data[0]['location'])
        createParkingMarkers(data.data)
    }).fail(function() {
        console.log( "coulnd't fetch data." );
  })
}

function createParkingMarkers(parkings){
    if (parking_markers) {
        parking_markers.forEach(function(marker){
            marker.setMap(null);
        })
        parking_markers = [];
    }
    parkings.forEach(function(parking){
        parking_markers.push(createMarker(parking))
    })
}

function createMarker(parking){
    var contentString = "<b>Location</b><br>" + parking.station_address;
    var image = '/static/plops_app/image/icon-parking.png'
    var marker = new google.maps.Marker({
        position: new google.maps.LatLng(parking.location_lat, parking.location_lng),
        map: map,
        icon: image,
        animation: google.maps.Animation.DROP,
        zIndex: Math.round(parking.location_lat*-100000)<<5,
    });
    
    google.maps.event.addListener(marker, 'click', function() {
        infowindow.setContent(contentString); 
        infowindow.open(map, marker);
    });    
    return marker;
}
