"use strict";

var slot_markers = [];

function findParking(latLng){
    console.log('finding parking.', latLng);
    var lat = latLng.lat().toString()
    var lng = latLng.lng().toString()
    var url = `/app/api?lat=${lat}&lng=${lng}`;
    console.log("url:" + url)
    $.get(url, function(data, status){
        createParkingMarkers(data.slots)
    }).fail(function() {
        console.log( "coulnd't fetch data." );
  })
}

function createParkingMarkers(slots){
    if (slot_markers) {
        slot_markers.forEach(function(marker){
            marker.setMap(null);
        })
        slot_markers = [];
    }
    slots.forEach(function(slot){
        slot_markers.push(createMarker(slot))
    })
}

function createMarker(slot){
    var contentString = "<b>Location</b><br>" + slot.station_address;
    var image = '/static/plops_app/image/icon-parking.png'
    var marker = new google.maps.Marker({
        position: new google.maps.LatLng(slot.location_lat, slot.location_lng),
        map: map,
        icon: image,
        animation: google.maps.Animation.DROP,
        zIndex: Math.round(slot.location_lat*-100000)<<5,
    });
    
    google.maps.event.addListener(marker, 'click', function() {
        infowindow.setContent(contentString); 
        infowindow.open(map, marker);
    });    
    return marker;
}
