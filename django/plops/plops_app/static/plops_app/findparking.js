"use strict";

var slot_markers = [];

function findParking(latLng){
    console.log('finding parking.', latLng);
    var lat = latLng.lat().toString()
    var lng = latLng.lng().toString()
    var url = `/api?lat=${lat}&lng=${lng}`;
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
    var date = new Date( Date.parse(slot.timestamp) );
    var popup = slot.station_address;
    popup = popup + "<br><b>"+ slot.distance_f +" feet away</b> ";
    popup = popup + "<br>Available / Total Spaces: ";
    popup = popup + "<b>" + slot.available_spots + " / " + slot.space_count + "</b>";
    popup = popup + "<br>Update time: " + date.toLocaleTimeString();
    var image;
    if (slot.available_spots > 0){
        image = '/static/plops_app/image/icon-parking.png'
    }else{
        image = '/static/plops_app/image/icon-full.png'
    }
    var marker = new google.maps.Marker({
        position: new google.maps.LatLng(slot.location_lat, slot.location_lng),
        map: map,
        icon: image,
        animation: google.maps.Animation.DROP,
        zIndex: Math.round(slot.location_lat*-100000)<<5,
    });
    
    google.maps.event.addListener(marker, 'click', function() {
        infowindow.setContent(popup); 
        infowindow.open(map, marker);
        query_for_charts(slot)
    });    
    return marker;
}
