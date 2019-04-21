"use strict";

var map;
var marker;
var infowindow;

function initMap() {
    // set center in Seattle
    var myOptions = {
        zoom: 14,
        center: new google.maps.LatLng(47.619293, -122.324732),
        mapTypeControl: true,
        mapTypeControlOptions: {style: google.maps.MapTypeControlStyle.DROPDOWN_MENU},
        navigationControl: true,
        mapTypeId: google.maps.MapTypeId.ROADMAP
    }
    // create map
    map = new google.maps.Map(
      document.getElementById('map'), myOptions);
    // The marker, positioned at Uluru
    
    google.maps.event.addListener(map, 'click', function(event) {
        // remove old marker
        if (marker) {
            marker.setMap(null);
            marker = null;
        }
        //call function to create marker
        marker = createSearchPointMarker(event.latLng, "name", "<b>Location</b><br>"+event.latLng);
        findParking(event.latLng);
    });

    infowindow = new google.maps.InfoWindow(
      { 
        size: new google.maps.Size(150,50)
    });

}

// A function to create the marker and set up the event window function 
function createSearchPointMarker(latlng, name, html) {
    var contentString = html;
    var marker = new google.maps.Marker({
        position: latlng,
        map: map,
        animation: google.maps.Animation.DROP,
        zIndex: Math.round(latlng.lat()*-100000)<<5,
        });

    google.maps.event.addListener(marker, 'click', function() {
        infowindow.setContent(contentString); 
        infowindow.open(map,marker);
        });
    
    //google.maps.event.trigger(marker, 'click');    
    return marker;
}